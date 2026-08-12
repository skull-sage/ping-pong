package com.pingpong.ping.infrastructure.persistence;

import com.pingpong.ping.domain.Ping;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Dummy repository that simulates a persistence store. {@code @WithSpan} makes {@link #save} appear
 * as its own INTERNAL span (via WithSpanAspect); the {@code Thread.sleep} injects a deliberate
 * write-latency "performance hazard" so the span stands out in the Tempo waterfall.
 */
@Repository
public class PingRepository {

    private static final Logger log = LoggerFactory.getLogger(PingRepository.class);

    @WithSpan("PingRepository.save")
    public void save(Ping ping) {
        sleep_ms(ThreadLocalRandom.current().nextInt(60, 140));
        log.debug("persisted ping id={} status={}", ping.id(), ping.status());
    }

    private static void sleep_ms(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
