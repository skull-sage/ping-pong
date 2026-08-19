package com.pingpong.ping.infrastructure.persistence;

import com.pingpong.ping.domain.Ping;
import io.micrometer.observation.annotation.Observed;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Dummy repository that simulates a persistence store. Micrometer's {@code @Observed} makes
 * {@link #save} appear as its own child span (via {@code ObservedAspect}) INSIDE the current trace,
 * so it inherits the single trace_id. The {@code contextualName} becomes the span name shown in
 * Tempo; the {@code name} is the metric/observation name (dot-separated) shown in Prometheus. The
 * {@code Thread.sleep} injects a deliberate write-latency "performance hazard" so the span stands
 * out in the waterfall.
 */
@Repository
public class PingRepository {

    private static final Logger log = LoggerFactory.getLogger(PingRepository.class);

    @Observed(name = "ping.repository.save", contextualName = "PingRepository.save")
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
