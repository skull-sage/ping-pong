package com.pingpong.ping.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.TracingAttributes;
import com.pingpong.ping.domain.Ping;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Outbound adapter (infrastructure layer). Builds a Micrometer PRODUCER span, stamps the
 * semantic-convention tags, injects the active W3C trace context into the Kafka headers, and sends.
 *
 * <p><b>Single-trace_id note:</b> the span is started with {@link Tracer#spanBuilder()} without a
 * parent override, so it becomes a CHILD of whatever span is currently active — here, the HTTP
 * SERVER span that opened the trace at {@code PingController}. We then
 * {@link Propagator#inject inject} <i>that same</i> trace context into the record headers, so the
 * downstream consumer continues the trace rather than starting a new one. Net effect: the trace_id
 * created on the REST request is the one correlator for the entire ping -> pong -> bang lifecycle.
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
        // Business saga id comes from Baggage (set at the controller), not the envelope body.
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
                .tag(TracingAttributes.DDD_BOUNDED_CONTEXT, Ping.BOUNDED_CONTEXT)
                .tag(TracingAttributes.DDD_AGGREGATE_TYPE, Ping.AGGREGATE_TYPE)
                .tag(TracingAttributes.DDD_AGGREGATE_ID, String.valueOf(envelope.data().get("pingId")))
                .start();

        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            String json = mapper.writeValueAsString(envelope);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, json);

            // Inject the ACTIVE trace context (traceparent/tracestate) into the Kafka headers so the
            // consumer CONTINUES this trace. span.context() is the context of the trace we are in.
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
