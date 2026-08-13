package com.pingpong.pong.presentation.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.Topics;
import com.pingpong.common.TracingAttributes;
import com.pingpong.pong.application.HandleFaultCommandHandler;
import com.pingpong.pong.application.command.HandleFaultCommand;
import com.pingpong.pong.domain.Pong;
import com.pingpong.pong.infrastructure.messaging.KafkaTracingSupport;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Presentation layer — inbound event adapter for the CR-2 <b>failure pipeline</b>. Each
 * {@code ping.faults} record starts a new root CONSUMER trace linked to service_ping's publish
 * span, then emits a {@link HandleFaultCommand} that deliberately throws.
 *
 * <p>The exception is logged at ERROR with the full stack trace and recorded on the span
 * ({@code recordException} + status = ERROR), which is exactly what makes it findable in Loki and
 * back-traceable in Tempo. It is intentionally <b>not</b> rethrown: swallowing after logging keeps
 * the demo to one clean error per message instead of triggering Kafka redelivery storms.
 */
@Component
public class FaultEventListener {

    private static final Logger log = LoggerFactory.getLogger(FaultEventListener.class);
    private static final String CONSUMER_GROUP = "service-pong-faults";

    private final HandleFaultCommandHandler handleFaultHandler;
    private final ObjectMapper mapper;
    private final OpenTelemetry openTelemetry;

    public FaultEventListener(HandleFaultCommandHandler handleFaultHandler,
                              ObjectMapper mapper,
                              OpenTelemetry openTelemetry) {
        this.handleFaultHandler = handleFaultHandler;
        this.mapper = mapper;
        this.openTelemetry = openTelemetry;
    }

    @KafkaListener(topics = Topics.PING_FAULTS, groupId = CONSUMER_GROUP)
    public void on_fault(ConsumerRecord<String, String> record) throws Exception {
        Context producerCtx = openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), record.headers(), KafkaTracingSupport.GETTER);
        SpanContext producerSpan = Span.fromContext(producerCtx).getSpanContext();

        EventEnvelope env = mapper.readValue(record.value(), EventEnvelope.class);

        Span span = openTelemetry.getTracer(Pong.RESPONDER)
                .spanBuilder("process " + Topics.PING_FAULTS)
                .setNoParent()
                .addLink(producerSpan)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .setAttribute(TracingAttributes.MESSAGING_OPERATION_TYPE, "process")
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_NAME, Topics.PING_FAULTS)
                .setAttribute(TracingAttributes.MESSAGING_CONSUMER_GROUP_NAME, CONSUMER_GROUP)
                .setAttribute(TracingAttributes.MESSAGING_KAFKA_OFFSET, record.offset())
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_PARTITION_ID, record.partition())
                .setAttribute(TracingAttributes.MESSAGING_MESSAGE_ID, env.eventId())
                .setAttribute(TracingAttributes.MESSAGING_CONVERSATION_ID, env.correlationId())
                .setAttribute(TracingAttributes.APP_CAUSATION_ID, env.eventId())
                .setAttribute(TracingAttributes.MESSAGE_TYPE, "event")
                .setAttribute(TracingAttributes.EVENT_TYPE, env.eventType())
                .setAttribute(TracingAttributes.DDD_BOUNDED_CONTEXT, Pong.BOUNDED_CONTEXT)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            MDC.put(TracingAttributes.MDC_CORRELATION_ID, env.correlationId());
            MDC.put(TracingAttributes.MDC_CAUSATION_ID, env.eventId());

            Map<String, Object> data = env.data();
            handleFaultHandler.handle(new HandleFaultCommand(
                    String.valueOf(data.get("pingId")),
                    String.valueOf(data.get("reason")),
                    env.correlationId(),
                    env.eventId()));
        } catch (Exception e) {
            // Record on the span (status=error) so Tempo shows it, and log at ERROR with the stack
            // trace so Loki carries it with traceId/correlationId for back-tracing.
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            log.error("Fault pipeline failed corr={} eventId={}: {}",
                    env.correlationId(), env.eventId(), e.getMessage(), e);
            // Intentionally swallowed after logging — see class javadoc.
        } finally {
            MDC.clear();
            span.end();
        }
    }
}
