package com.pingpong.pong.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.TracingAttributes;
import com.pingpong.pong.domain.Pong;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Outbound adapter (infrastructure layer). Because it runs inside the CONSUMER span's scope (opened
 * by {@code PingEventListener}), this PRODUCER span is a child of that consumer span and therefore
 * stays in the SAME trace that arrived from service_ping. Injecting the context into the outgoing
 * headers hands that same trace_id to service_bang — the chain never breaks the trace.
 */
@Component
@SuppressWarnings({"rawtypes", "unchecked"})
public class KafkaEventPublisher {

    // Raw type on purpose: Spring Boot auto-configures a KafkaTemplate<?, ?> bean, which a
    // KafkaTemplate<String, String> injection point will not match. Raw matches it.
    private final KafkaTemplate kafka;
    private final ObjectMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;

    public KafkaEventPublisher(KafkaTemplate kafka,
                               ObjectMapper mapper,
                               Tracer tracer,
                               Propagator propagator) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    public void publish(String topic, String partitionKey, EventEnvelope envelope) {
        // Business saga id flows in from Baggage (extracted from the inbound Kafka headers), not the body.
        String sagaId = tracer.getBaggage(TracingAttributes.BAGGAGE_CORRELATION_ID).get();

        Span span = tracer.spanBuilder()
                .name("publish " + topic)
                .kind(Span.Kind.PRODUCER)
                .tag(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .tag(TracingAttributes.MESSAGING_OPERATION_TYPE, "publish")
                .tag(TracingAttributes.MESSAGING_DESTINATION_NAME, topic)
                .tag(TracingAttributes.MESSAGING_KAFKA_MESSAGE_KEY, partitionKey)
                .tag(TracingAttributes.MESSAGING_MESSAGE_ID, envelope.eventId())
                .tag(TracingAttributes.MESSAGING_CONVERSATION_ID, sagaId == null ? "" : sagaId)
                .tag(TracingAttributes.APP_CAUSATION_ID, envelope.causationId())
                .tag(TracingAttributes.MESSAGE_TYPE, "event")
                .tag(TracingAttributes.MESSAGE_CATEGORY, envelope.messageCategory())
                .tag(TracingAttributes.EVENT_TYPE, envelope.eventType())
                .tag(TracingAttributes.EVENT_VERSION, envelope.eventVersion())
                .tag(TracingAttributes.DDD_BOUNDED_CONTEXT, Pong.BOUNDED_CONTEXT)
                .tag(TracingAttributes.DDD_AGGREGATE_TYPE, Pong.AGGREGATE_TYPE)
                .tag(TracingAttributes.DDD_AGGREGATE_ID, String.valueOf(envelope.data().get("pingId")))
                .start();

        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            String json = mapper.writeValueAsString(envelope);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, json);

            // Hand the SAME trace context (arrived from ping, kept current) to service_bang.
            propagator.inject(span.context(), record.headers(), KafkaTracingSupport.SETTER);

            kafka.send(record);
        } catch (Exception e) {
            span.error(e);
            throw new IllegalStateException("Failed to publish event to " + topic, e);
        } finally {
            span.end();
        }
    }
}
