package com.pingpong.bang.infrastructure.persistence;

import com.pingpong.bang.domain.Pong;
import io.micrometer.observation.annotation.Observed;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Dummy repository; Micrometer's {@code @Observed} + sleep simulate a slow write as its own child
 * span inside the current trace (inherits the single trace_id propagated from ping -> pong).
 */
@Repository
public class PongRepository {

    private static final Logger log = LoggerFactory.getLogger(PongRepository.class);

    @Observed(name = "bang.repository.save", contextualName = "PongRepository.save")
    public void save(Pong pong) {
        sleep_ms(ThreadLocalRandom.current().nextInt(60, 140));
        log.debug("persisted pong pingId={} status={}", pong.ping_id(), pong.status());
    }

    private static void sleep_ms(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
