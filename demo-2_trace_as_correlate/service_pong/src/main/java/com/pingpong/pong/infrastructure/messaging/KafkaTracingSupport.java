package com.pingpong.pong.infrastructure.messaging;

import io.micrometer.tracing.propagation.Propagator;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Injects/extracts the W3C trace context on Kafka headers using Micrometer Tracing's
 * {@link Propagator.Setter}/{@link Propagator.Getter}. Carrying the context is what lets a consumer
 * CONTINUE the same trace, so one trace_id spans the whole ping -> pong -> bang flow.
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
