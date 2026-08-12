package com.pingpong.bang.infrastructure.persistence;

import com.pingpong.bang.domain.Pong;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/** Dummy repository; {@code @WithSpan} + sleep simulate a slow write as its own INTERNAL span. */
@Repository
public class PongRepository {

    private static final Logger log = LoggerFactory.getLogger(PongRepository.class);

    @WithSpan("PongRepository.save")
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
