package com.pingpong.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/**
 * The stable cross-service contract.
 *
 * <p><b>Correlation is NOT carried here.</b> Per the OpenTelemetry traces model, cross-service
 * correlation is the job of the {@code trace_id}, propagated as W3C trace context in the Kafka
 * transport headers (see {@code KafkaTracingSupport}). A human-readable business <b>saga id</b> is
 * likewise not a body field: it travels as OpenTelemetry <b>Baggage</b> (key {@code correlationId}),
 * which rides in the same propagated context and is surfaced in logs via MDC. Keeping both out of
 * the body avoids a second, competing "correlation id" and keeps this record free of tracing types.
 *
 * <p>{@code causationId} remains as pure <b>domain</b> metadata — "which message caused this one" —
 * which is a business fact distinct from the trace's own parent/child structure.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope(
        String eventId,          // readable id; also messaging.message.id + idempotency key
        String eventType,        // -> span attribute event.type (e.g. PingCreated)
        String eventVersion,     // schema evolution
        String messageCategory,  // domain | integration
        String causationId,      // id of the message that caused this -> app.causation_id (domain metadata)
        Instant occurredAt,
        String producer,         // origin service name
        Map<String, Object> data // business payload (ids only, no PII)
) {

    public static Builder builder() {
        return new Builder();
    }

    /** Small fluent builder; keeps call sites readable without pulling in a framework. */
    public static final class Builder {
        private String eventId;
        private String eventType;
        private String eventVersion = "1.0";
        private String messageCategory = "integration";
        private String causationId;
        private Instant occurredAt = Instant.now();
        private String producer;
        private Map<String, Object> data = Map.of();

        public Builder event_id(String v) { this.eventId = v; return this; }
        public Builder event_type(String v) { this.eventType = v; return this; }
        public Builder event_version(String v) { this.eventVersion = v; return this; }
        public Builder message_category(String v) { this.messageCategory = v; return this; }
        public Builder causation_id(String v) { this.causationId = v; return this; }
        public Builder occurred_at(Instant v) { this.occurredAt = v; return this; }
        public Builder producer(String v) { this.producer = v; return this; }
        public Builder data(Map<String, Object> v) { this.data = v; return this; }

        public EventEnvelope build() {
            return new EventEnvelope(eventId, eventType, eventVersion, messageCategory,
                    causationId, occurredAt, producer, data);
        }
    }
}
