package com.pingpong.ponga.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.TracingAttributes;
import com.pingpong.ponga.application.port.out.EventPublisher;
import com.pingpong.ponga.domain.Pong;
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
 * PRODUCER adapter. Because it runs inside the consumer span's scope, this publish span joins the
 * consumer's (new) trace, so the pong hop is visible within pong-a's trace and linked to the saga.
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
        this.tracer = openTelemetry.getTracer(Pong.RESPONDER);
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
                .setAttribute(TracingAttributes.DDD_BOUNDED_CONTEXT, Pong.BOUNDED_CONTEXT)
                .setAttribute(TracingAttributes.DDD_AGGREGATE_TYPE, Pong.AGGREGATE_TYPE)
                .setAttribute(TracingAttributes.DDD_AGGREGATE_ID, String.valueOf(envelope.data().get("pingId")))
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            String json = mapper.writeValueAsString(envelope);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, json);
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
