import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple concurrent HTTP ping trigger for the simulation — no dependencies, single file.
 *
 * <p>Fires {@code --requests} POST calls to service_ping across a pool of {@code --concurrency}
 * worker threads, so multiple ping-pong sagas run in parallel and produce a rich set of traces
 * in Grafana Tempo.
 *
 * <pre>
 *   java Trigger.java                                  # 20 requests, 5 concurrent, localhost:8080
 *   java Trigger.java --requests 200 --concurrency 20  # heavier load
 *   java Trigger.java --url http://localhost:8080/api/ping --requests 50 --concurrency 10
 * </pre>
 */
public class Trigger {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parse_args(args);
        String url = opts.getOrDefault("url", "http://localhost:8080/api/ping");
        int requests = Integer.parseInt(opts.getOrDefault("requests", "20"));
        int concurrency = Integer.parseInt(opts.getOrDefault("concurrency", "5"));

        System.out.printf("Firing %d requests to %s with concurrency %d%n", requests, url, concurrency);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicLong total_latency_ms = new AtomicLong();

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        long started = System.nanoTime();

        List<Callable<Void>> tasks = new java.util.ArrayList<>();
        for (int i = 0; i < requests; i++) {
            final int seq = i;
            tasks.add(() -> {
                fire_one(client, url, seq, ok, failed, total_latency_ms);
                return null;
            });
        }

        List<Future<Void>> results = pool.invokeAll(tasks);
        for (Future<Void> f : results) {
            try {
                f.get();
            } catch (Exception ignored) {
                // per-request failures are already counted in fire_one
            }
        }
        pool.shutdown();

        long elapsed_ms = (System.nanoTime() - started) / 1_000_000;
        int done = ok.get() + failed.get();
        System.out.println("------------------------------------------------------------");
        System.out.printf("Done: %d ok, %d failed in %d ms%n", ok.get(), failed.get(), elapsed_ms);
        if (done > 0) {
            System.out.printf("Avg request latency: %d ms | throughput: %.1f req/s%n",
                    total_latency_ms.get() / done, done * 1000.0 / Math.max(elapsed_ms, 1));
        }
    }

    private static void fire_one(HttpClient client, String url, int seq,
                                 AtomicInteger ok, AtomicInteger failed, AtomicLong total_latency_ms) {
        String body = "{\"note\":\"ping-" + seq + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        long t0 = System.nanoTime();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            total_latency_ms.addAndGet((System.nanoTime() - t0) / 1_000_000);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                ok.incrementAndGet();
            } else {
                failed.incrementAndGet();
                System.out.printf("  [%d] HTTP %d: %s%n", seq, response.statusCode(), response.body());
            }
        } catch (Exception e) {
            failed.incrementAndGet();
            System.out.printf("  [%d] error: %s%n", seq, e.getMessage());
        }
    }

    private static Map<String, String> parse_args(String[] args) {
        Map<String, String> opts = new java.util.HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].startsWith("--")) {
                opts.put(args[i].substring(2), args[i + 1]);
            }
        }
        return opts;
    }
}
