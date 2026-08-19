package com.pingpong.ping.infrastructure.service;

import io.micrometer.observation.annotation.Observed;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dummy downstream infrastructure service. Micrometer's {@code @Observed} on {@link #record}
 * produces a child span inside the current trace (same trace_id); the longer {@code Thread.sleep}
 * simulates a slow external dependency, which is the main "performance hazard" you can spot in a
 * trace and correlate with the {@code ping.audit.record} latency metric.
 */
@Service
public class PingAuditService {

    private static final Logger log = LoggerFactory.getLogger(PingAuditService.class);

    @Observed(name = "ping.audit.record", contextualName = "PingAuditService.record")
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
