package com.pingpong.ping.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.TracingAttributes;
import com.pingpong.ping.application.port.out.EventPublisher;
import com.pingpong.ping.domain.Ping;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Outbound adapter. Builds a PRODUCER span with the full semantic-convention attribute set
 * (§7.5, §16), injects the W3C context into the record headers, and sends. The span is a child of
 * whatever context is current (the inbound SERVER span), so it stays in the same trace (CR-1).
 */
@Component
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafka,
                               ObjectMapper mapper,
                               OpenTelemetry openTelemetry) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("service-ping");
    }

    @Override
    public void publish(String topic, String partitionKey, EventEnvelope envelope) {
        Span span = tracer.spanBuilder("publish " + topic)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .setAttribute(TracingAttributes.MESSAGING_OPERATION_TYPE, "publish")
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_NAME, topic)
                .setAttribute(TracingAttributes.MESSAGING_KAFKA_MESSAGE_KEY, partitionKey)
                .setAttribute(TracingAttributes.MESSAGING_MESSAGE_ID, envelope.eventId())
                .setAttribute(TracingAttributes.MESSAGING_CONVERSATION_ID, envelope.correlationId())
                .setAttribute(TracingAttributes.APP_CAUSATION_ID, envelope.causationId())
                .setAttribute(TracingAttributes.MESSAGE_TYPE, "event")
                .setAttribute(TracingAttributes.MESSAGE_CATEGORY, envelope.messageCategory())
                .setAttribute(TracingAttributes.EVENT_TYPE, envelope.eventType())
                .setAttribute(TracingAttributes.EVENT_VERSION, envelope.eventVersion())
                .setAttribute(TracingAttributes.DDD_BOUNDED_CONTEXT, Ping.BOUNDED_CONTEXT)
                .setAttribute(TracingAttributes.DDD_AGGREGATE_TYPE, Ping.AGGREGATE_TYPE)
                .setAttribute(TracingAttributes.DDD_AGGREGATE_ID, String.valueOf(envelope.data().get("pingId")))
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            String json = mapper.writeValueAsString(envelope);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, json);

            // Inject the current W3C trace context into the Kafka transport headers.
            openTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), record.headers(), KafkaTracingSupport.SETTER);

            kafka.send(record);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw new IllegalStateException("Failed to publish event to " + topic, e);
        } finally {
            span.end();
        }
    }
}
