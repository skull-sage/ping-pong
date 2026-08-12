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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent HTTP ping trigger for the simulation — no dependencies, single file.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Continuous (default):</b> a pool of {@code --concurrency} virtual users keeps spawning
 *       requests forever, each pausing a randomized "think time" between calls to mimic real-time
 *       user traffic. Prints live throughput/latency stats every {@code --report-sec}. Stop with
 *       Ctrl+C (a shutdown hook prints a final summary).</li>
 *   <li><b>Bounded:</b> pass {@code --requests N} to fire exactly N requests and exit.</li>
 * </ul>
 *
 * <pre>
 *   java Trigger.java                                    # continuous, 10 users, ~250ms think time
 *   java Trigger.java --concurrency 25 --think-ms 100    # heavier continuous load
 *   java Trigger.java --duration-sec 60                  # continuous, auto-stop after 60s
 *   java Trigger.java --requests 200 --concurrency 20    # bounded: 200 requests then exit
 * </pre>
 */
public class Trigger {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // Shared counters (cumulative over the whole run).
    private static final AtomicInteger ok = new AtomicInteger();
    private static final AtomicInteger failed = new AtomicInteger();
    private static final AtomicLong total_latency_ms = new AtomicLong();
    private static final AtomicInteger seq = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parse_args(args);
        String url = opts.getOrDefault("url", "http://localhost:8080/api/ping");
        int concurrency = Integer.parseInt(opts.getOrDefault("concurrency", "10"));
        int requests = Integer.parseInt(opts.getOrDefault("requests", "0"));       // 0 => continuous
        int think_ms = Integer.parseInt(opts.getOrDefault("think-ms", "250"));
        int report_sec = Integer.parseInt(opts.getOrDefault("report-sec", "5"));
        int duration_sec = Integer.parseInt(opts.getOrDefault("duration-sec", "0")); // 0 => until Ctrl+C

        if (requests > 0) {
            run_bounded(url, requests, concurrency);
        } else {
            run_continuous(url, concurrency, think_ms, report_sec, duration_sec);
        }
    }

    // ---- Continuous mode: mimic real-time user traffic until Ctrl+C (or --duration-sec) ----
    private static void run_continuous(String url, int concurrency, int think_ms,
                                       int report_sec, int duration_sec) throws Exception {
        System.out.printf("Continuous load -> %s | users=%d, think=%dms, report every %ds%s%n",
                url, concurrency, think_ms, report_sec,
                duration_sec > 0 ? (", auto-stop after " + duration_sec + "s") : " (Ctrl+C to stop)");
        System.out.println("------------------------------------------------------------");

        AtomicBoolean running = new AtomicBoolean(true);
        long started = System.nanoTime();

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        for (int i = 0; i < concurrency; i++) {
            pool.submit(() -> {
                while (running.get()) {
                    fire_one(url, seq.getAndIncrement());
                    // Randomized think time (0.5x .. 1.5x) so requests arrive irregularly.
                    long jitter = (long) (think_ms * (0.5 + ThreadLocalRandom.current().nextDouble()));
                    try {
                        Thread.sleep(jitter);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
        }

        // Print a final summary once, whether we stop via Ctrl+C or via --duration-sec.
        AtomicBoolean summarized = new AtomicBoolean(false);
        Runnable summary = () -> {
            if (summarized.compareAndSet(false, true)) {
                running.set(false);
                long elapsed_ms = (System.nanoTime() - started) / 1_000_000;
                int done = ok.get() + failed.get();
                System.out.println("\n------------------------------------------------------------");
                System.out.printf("STOPPED. Total: %d ok, %d failed in %.1fs%n",
                        ok.get(), failed.get(), elapsed_ms / 1000.0);
                if (done > 0) {
                    System.out.printf("Overall: %.1f req/s | avg latency %d ms%n",
                            done * 1000.0 / Math.max(elapsed_ms, 1), total_latency_ms.get() / done);
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(summary));

        // Live reporter loop (interval stats).
        int last_ok = 0, last_failed = 0;
        long last_time = System.nanoTime();
        long deadline = duration_sec > 0 ? started + duration_sec * 1_000_000_000L : Long.MAX_VALUE;
        while (running.get() && System.nanoTime() < deadline) {
            Thread.sleep(report_sec * 1000L);
            int cur_ok = ok.get(), cur_failed = failed.get();
            long now = System.nanoTime();
            double window_s = (now - last_time) / 1_000_000_000.0;
            int window_done = (cur_ok - last_ok) + (cur_failed - last_failed);
            System.out.printf("[t+%3.0fs] %6d ok, %4d failed | window %.1f req/s (%d in %.1fs)%n",
                    (now - started) / 1_000_000_000.0, cur_ok, cur_failed,
                    window_done / window_s, window_done, window_s);
            last_ok = cur_ok;
            last_failed = cur_failed;
            last_time = now;
        }

        summary.run();
        pool.shutdownNow();
    }

    // ---- Bounded mode: fire exactly N requests then exit ----
    private static void run_bounded(String url, int requests, int concurrency) throws Exception {
        System.out.printf("Firing %d requests to %s with concurrency %d%n", requests, url, concurrency);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        long started = System.nanoTime();

        List<Callable<Void>> tasks = new java.util.ArrayList<>();
        for (int i = 0; i < requests; i++) {
            final int s = i;
            tasks.add(() -> {
                fire_one(url, s);
                return null;
            });
        }
        List<Future<Void>> results = pool.invokeAll(tasks);
        for (Future<Void> f : results) {
            try {
                f.get();
            } catch (Exception ignored) {
                // failures already counted in fire_one
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

    private static void fire_one(String url, int id) {
        String body = "{\"note\":\"ping-" + id + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        long t0 = System.nanoTime();
        try {
            HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            total_latency_ms.addAndGet((System.nanoTime() - t0) / 1_000_000);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                ok.incrementAndGet();
            } else {
                failed.incrementAndGet();
                System.out.printf("  [%d] HTTP %d: %s%n", id, response.statusCode(), response.body());
            }
        } catch (Exception e) {
            failed.incrementAndGet();
            System.out.printf("  [%d] error: %s%n", id, e.getMessage());
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
