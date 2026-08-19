package com.pingpong.bang.infrastructure.service;

import io.micrometer.observation.annotation.Observed;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dummy downstream processing service; Micrometer's {@code @Observed} + a longer sleep simulate the
 * main "performance hazard" span within the current trace (same trace_id propagated from ping -> pong).
 */
@Service
public class PongProcessingService {

    private static final Logger log = LoggerFactory.getLogger(PongProcessingService.class);

    @Observed(name = "bang.processing.process", contextualName = "PongProcessingService.process")
    public void process(String pingId) {
        sleep_ms(ThreadLocalRandom.current().nextInt(120, 260));
        log.debug("processed ping pingId={}", pingId);
    }

    private static void sleep_ms(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
