package com.pingpong.bang.infrastructure.service;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dummy downstream processing service; {@code @WithSpan} + a longer sleep simulate the main
 * "performance hazard" span within bang's trace.
 */
@Service
public class PongProcessingService {

    private static final Logger log = LoggerFactory.getLogger(PongProcessingService.class);

    @WithSpan("PongProcessingService.process")
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
