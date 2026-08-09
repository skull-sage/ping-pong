package com.pingpong.pongb.infrastructure.messaging;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** Injects/extracts the W3C trace context on Kafka headers (CR-1). */
final class KafkaTracingSupport {

    static final TextMapSetter<Headers> SETTER =
            (headers, key, value) -> {
                if (headers != null) {
                    headers.remove(key);
                    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
                }
            };

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
