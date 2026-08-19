package com.pingpong.common;

/**
 * The single shared semantic-convention catalog (OQ-4).
 *
 * <p>Every service stamps these exact attribute names on its messaging spans so one query can
 * follow a business transaction across all of them. Names match {@code data_modeling.md} §8.3 and
 * {@code distributed_tracing.md} §16. Keeping them in one constants class is what makes the
 * naming machine-checkable and prevents cross-service drift.
 */
public final class TracingAttributes {

    // --- Messaging (OpenTelemetry semantic conventions) ---
    public static final String MESSAGING_SYSTEM = "messaging.system";
    public static final String MESSAGING_OPERATION_TYPE = "messaging.operation.type";
    public static final String MESSAGING_DESTINATION_NAME = "messaging.destination.name";
    public static final String MESSAGING_DESTINATION_PARTITION_ID = "messaging.destination.partition.id";
    public static final String MESSAGING_KAFKA_OFFSET = "messaging.kafka.offset";
    public static final String MESSAGING_KAFKA_MESSAGE_KEY = "messaging.kafka.message.key";
    public static final String MESSAGING_CONSUMER_GROUP_NAME = "messaging.consumer.group.name";
    public static final String MESSAGING_MESSAGE_ID = "messaging.message.id";
    /** Business saga id, sourced from Baggage and stamped on messaging spans for queryability. */
    public static final String MESSAGING_CONVERSATION_ID = "messaging.message.conversation_id";

    // --- Business correlation (domain-owned) ---
    /** Custom: the id of the message that directly caused this one (cause -> effect). */
    public static final String APP_CAUSATION_ID = "app.causation_id";

    /**
     * OpenTelemetry Baggage key carrying the human-readable business saga id across the whole flow.
     * It is NOT the correlator (the {@code trace_id} is); it is a convenience id for humans. Set once
     * at the origin (PingController), it auto-propagates in the trace context (incl. over Kafka) and
     * is surfaced in logs via the {@code management.tracing.baggage.correlation.fields} MDC bridge.
     */
    public static final String BAGGAGE_CORRELATION_ID = "correlationId";

    // --- Message classification ---
    public static final String MESSAGE_TYPE = "message.type";           // command | event
    public static final String MESSAGE_CATEGORY = "message.category";   // domain | integration
    public static final String EVENT_TYPE = "event.type";
    public static final String EVENT_VERSION = "event.version";

    // --- DDD tags (DD-1) ---
    public static final String DDD_BOUNDED_CONTEXT = "ddd.bounded_context";
    public static final String DDD_AGGREGATE_TYPE = "ddd.aggregate.type";
    public static final String DDD_AGGREGATE_ID = "ddd.aggregate.id";

    // --- Delivery integrity ---
    public static final String MESSAGING_REDELIVERED = "messaging.redelivered";
    public static final String MESSAGING_DELIVERY_ATTEMPTS = "messaging.delivery_attempts";

    // --- Span event names + their attribute keys ---
    public static final String EVENT_DUPLICATE_DETECTED = "messaging.duplicate_detected";
    public static final String IDEMPOTENCY_OUTCOME = "idempotency.outcome"; // processed | skipped

    // --- MDC keys ---
    // Note: the "correlationId" MDC key is populated automatically from Baggage (see
    // BAGGAGE_CORRELATION_ID) via the tracing correlation-fields bridge — no manual MDC.put needed.
    public static final String MDC_CAUSATION_ID = "causationId";

    private TracingAttributes() {
    }
}
