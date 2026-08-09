package com.pingpong.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/**
 * The stable cross-service contract (see {@code distributed_tracing.md} §11).
 *
 * <p>Carries the <b>business</b> correlation ids ({@code correlationId}, {@code causationId}) in the
 * body so the domain owns them. The <b>trace context</b> ({@code traceparent}/{@code tracestate})
 * never lives here — it rides in the Kafka transport headers. This record contains no OTel types,
 * keeping the domain pure (OQ-2).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope(
        String eventId,          // readable id; also messaging.message.id + idempotency key
        String eventType,        // -> span attribute event.type (e.g. PingCreated)
        String eventVersion,     // schema evolution (DD-4)
        String messageCategory,  // domain | integration (DD-2)
        String correlationId,    // saga id -> messaging.message.conversation_id (kept across the flow)
        String causationId,      // id of the message that caused this -> app.causation_id
        Instant occurredAt,
        String producer,         // origin service name
        Map<String, Object> data // business payload (ids only, no PII — OQ-5)
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
        private String correlationId;
        private String causationId;
        private Instant occurredAt = Instant.now();
        private String producer;
        private Map<String, Object> data = Map.of();

        public Builder event_id(String v) { this.eventId = v; return this; }
        public Builder event_type(String v) { this.eventType = v; return this; }
        public Builder event_version(String v) { this.eventVersion = v; return this; }
        public Builder message_category(String v) { this.messageCategory = v; return this; }
        public Builder correlation_id(String v) { this.correlationId = v; return this; }
        public Builder causation_id(String v) { this.causationId = v; return this; }
        public Builder occurred_at(Instant v) { this.occurredAt = v; return this; }
        public Builder producer(String v) { this.producer = v; return this; }
        public Builder data(Map<String, Object> v) { this.data = v; return this; }

        public EventEnvelope build() {
            return new EventEnvelope(eventId, eventType, eventVersion, messageCategory,
                    correlationId, causationId, occurredAt, producer, data);
        }
    }
}
