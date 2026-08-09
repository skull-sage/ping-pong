package com.pingpong.ping.infrastructure.messaging;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * The only place trace context touches Kafka headers. Injects the active W3C context on publish
 * and extracts it on receive (CR-1). Business ids travel in the envelope body, not here (§9, §13).
 */
final class KafkaTracingSupport {

    /** Writes {@code traceparent}/{@code tracestate} into the outgoing record headers. */
    static final TextMapSetter<Headers> SETTER =
            (headers, key, value) -> {
                if (headers != null) {
                    headers.remove(key);
                    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
                }
            };

    /** Reads the same headers back on the consumer side. */
    static final TextMapGetter<Headers> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers headers) {
            return () -> StreamSupport.stream(headers.spliterator(), false)
                    .map(Header::key).iterator();
        }

        @Override
        public String get(Headers headers, String key) {
            if (headers == null) {
                return null;
            }
            Header h = headers.lastHeader(key);
            return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
        }
    };

    private KafkaTracingSupport() {
    }
}
