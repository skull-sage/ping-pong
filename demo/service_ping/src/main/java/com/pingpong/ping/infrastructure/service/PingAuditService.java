package com.pingpong.ping.infrastructure.service;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dummy downstream infrastructure service. {@code @WithSpan} on {@link #record} produces an
 * INTERNAL span; the longer {@code Thread.sleep} simulates a slow external dependency, which is the
 * main "performance hazard" you can spot in a trace and correlate with latency metrics.
 */
@Service
public class PingAuditService {

    private static final Logger log = LoggerFactory.getLogger(PingAuditService.class);

    @WithSpan("PingAuditService.record")
    public void record(String pingId, String note) {
        sleep_ms(ThreadLocalRandom.current().nextInt(120, 260));
        log.debug("audited ping id={} note={}", pingId, note);
    }

    private static void sleep_ms(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
