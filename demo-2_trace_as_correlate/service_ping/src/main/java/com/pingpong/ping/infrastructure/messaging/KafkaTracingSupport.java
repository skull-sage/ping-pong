package com.pingpong.ping.infrastructure.messaging;

import io.micrometer.tracing.propagation.Propagator;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * The only place trace context touches Kafka headers. It carries the W3C trace context
 * ({@code traceparent}/{@code tracestate}) so a downstream consumer can CONTINUE the very same
 * trace — this is the mechanism that keeps ONE trace_id alive across every async hop.
 *
 * <p>Uses Micrometer Tracing's {@link Propagator.Setter}/{@link Propagator.Getter} (not the raw
 * OpenTelemetry {@code TextMap*} API). Business ids travel in the envelope body, never here.
 */
public final class KafkaTracingSupport {

    /** Writes {@code traceparent}/{@code tracestate} into the outgoing record headers (on publish). */
    public static final Propagator.Setter<Headers> SETTER =
            (headers, key, value) -> {
                if (headers != null && value != null) {
                    headers.remove(key);
                    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
                }
            };

    /** Reads the same headers back on the consumer side (on receive). */
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
