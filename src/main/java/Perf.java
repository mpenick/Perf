import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by michaelpenick on 9/8/16.
 */
public class Perf {
    static final int NUM_THREADS = 1;
    static final int NUM_ITERATIONS = 1000 / NUM_THREADS;
    static final int NUM_CONCURRENT_REQUESTS = 10000;
    static final String INSERT_QUERY = "INSERT INTO stress.songs (id, title) VALUES (?, ?)";

    public static class TrustAllX509TrustManager implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }

        public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }
    }

    public static void run(Session session,
                           final PreparedStatement prepared,
                           final int numIterations) {
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(NUM_CONCURRENT_REQUESTS);

        for (int j = 0; j < numIterations; ++j) {
            for (int i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
                BoundStatement statement = prepared.bind(UUIDs.timeBased(), "a");
                futures.add(session.executeAsync(statement));
            }
            Futures.successfulAsList(futures);
            futures.clear();
        }
    }

    public static void main(String args[]) throws InterruptedException, ExecutionException, NoSuchAlgorithmException, KeyManagementException {

        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> futures = Lists.newArrayList();

        int numIterations = NUM_ITERATIONS;
        int numThreads = NUM_THREADS;
        String contactPoints = "127.0.0.1";
        boolean useSSL = false;

        for (int i = 0; i < args.length; ++i) {
            String arg = args[i].toLowerCase();
            switch(arg) {
                case "--ssl":
                    useSSL = true;
                    break;

                case "--iterations":
                    if (i + 1 > args.length) {
                        System.err.println("--iterations expects an integer argument");
                        System.exit(-1);
                    }
                    try {
                        numIterations = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException e) {
                        System.err.println("--iterations expects an integer argument");
                        System.exit(-1);
                    }
                    i++;
                    break;

                case "--threads":
                    if (i + 1 > args.length) {
                        System.err.println("--threads expects an integer argument");
                        System.exit(-1);
                    }
                    try {
                        numThreads = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException e) {
                        System.err.println("--threads expects an integer argument");
                        System.exit(-1);
                    }
                    i++;
                    break;

                case "--contacts":
                    if (i + 1 > args.length) {
                        System.err.println("--contacts expects a string argument");
                        System.exit(-1);
                    }
                    contactPoints = args[i + 1];
                    i++;
                    break;

                default:
                    System.err.printf("Unexpected argument '%s'\n", arg);
                    break;
            }
        }

        numIterations /= numThreads;


        System.out.printf("running with %d threads and %d iterations (ssl: %s, contacts: %s)\n",
                numThreads, numIterations, (useSSL ? "yes" : "no"), contactPoints);

        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(contactPoints.split(","));

        if (useSSL) {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] { new TrustAllX509TrustManager() }, new java.security.SecureRandom());
            JdkSSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(sc).build();
            builder = builder.withSSL(sslOptions);
        }

        try(Cluster cluster = builder.build()) {
            try (Session session = cluster.connect()) {
                session.execute("CREATE KEYSPACE IF NOT EXISTS stress WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
                session.execute("CREATE TABLE IF NOT EXISTS stress.songs (id uuid PRIMARY KEY, title text)");
                session.execute("TRUNCATE stress.songs");

                Thread.sleep(3000);

                final PreparedStatement prepared = session.prepare(INSERT_QUERY);

                long start = System.currentTimeMillis();
                for (int i = 0; i < numThreads; ++i) {
                    final int finalNumIterations = numIterations;
                    futures.add(executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            Perf.run(session, prepared, finalNumIterations);
                        }
                    }));
                }

                executor.shutdown();

                while(!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    Timer requestsTimer = cluster.getMetrics().getRequestsTimer();
                    System.out.printf("rate stats (requests/second): mean %f 1m %f 5m %f 15m %f\n",
                            requestsTimer.getMeanRate(),
                            requestsTimer.getOneMinuteRate(),
                            requestsTimer.getFiveMinuteRate(),
                            requestsTimer.getFifteenMinuteRate());
                }


                System.out.printf("inserted %d rows in %f seconds\n",
                        numThreads * numIterations * NUM_CONCURRENT_REQUESTS,
                        (System.currentTimeMillis() - start) / 1000.0);


                Snapshot snapshot = cluster.getMetrics().getRequestsTimer().getSnapshot();
                System.out.printf("final stats (microseconds): min %d max %d median %f 75th %f 98th %f 99th %f 99.9th %f\n",
                        snapshot.getMin(),
                        snapshot.getMax(),
                        snapshot.getMedian(),
                        snapshot.get75thPercentile(),
                        snapshot.get98thPercentile(),
                        snapshot.get99thPercentile(),
                        snapshot.get999thPercentile());
            }
        }
    }
}
