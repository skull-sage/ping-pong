package com.pingpong.bang.infrastructure.messaging;

import io.micrometer.tracing.propagation.Propagator;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Extracts the W3C trace context from Kafka headers using Micrometer Tracing's
 * {@link Propagator.Getter}. bang is the terminal consumer, so it only reads the context (to
 * CONTINUE the trace); the {@link Propagator.Setter} is kept for symmetry but publishes nothing.
 */
public final class KafkaTracingSupport {

    public static final Propagator.Setter<Headers> SETTER =
            (headers, key, value) -> {
                if (headers != null && value != null) {
                    headers.remove(key);
                    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
                }
            };

    public static final Propagator.Getter<Headers> GETTER =
            (headers, key) -> {
                if (headers == null) {
                    return null;
                }
                Header h = headers.lastHeader(key);
                return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
            };

    private KafkaTracingSupport() {
    }
}
