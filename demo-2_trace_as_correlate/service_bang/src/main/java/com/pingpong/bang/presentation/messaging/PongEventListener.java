package com.pingpong.bang.presentation.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.bang.application.HandlePongCommandHandler;
import com.pingpong.bang.application.command.HandlePongCommand;
import com.pingpong.bang.domain.Pong;
import com.pingpong.bang.infrastructure.messaging.KafkaTracingSupport;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.Topics;
import com.pingpong.common.TracingAttributes;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Presentation layer — inbound event adapter for the terminal link of the ping → pong → bang chain.
 *
 * <p><b>Single-trace_id note:</b> it {@link Propagator#extract extracts} the W3C context that
 * service_pong injected and uses it as the parent of this CONSUMER span, so the span CONTINUES the
 * one trace that began at service_ping's REST controller. By the time the flow reaches here, all
 * three services share the SAME trace_id — one Tempo trace shows the entire lifecycle end-to-end.
 *
 * <p>It dedups on the message id, then emits a {@link HandlePongCommand} to the application handler.
 */
@Component
public class PongEventListener {

    private static final Logger log = LoggerFactory.getLogger(PongEventListener.class);
    private static final String CONSUMER_GROUP = "service-bang";

    private final Set<String> processed_event_ids = ConcurrentHashMap.newKeySet();

    private final HandlePongCommandHandler handlePongHandler;
    private final ObjectMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;
    private final String serviceName;

    public PongEventListener(HandlePongCommandHandler handlePongHandler,
                             ObjectMapper mapper,
                             Tracer tracer,
                             Propagator propagator,
                             @Value("${spring.application.name}") String serviceName) {
        this.handlePongHandler = handlePongHandler;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
        this.serviceName = serviceName;
    }

    @KafkaListener(topics = Topics.PONG_EVENTS, groupId = CONSUMER_GROUP)
    public void on_pong(ConsumerRecord<String, String> record) throws Exception {
        EventEnvelope env = mapper.readValue(record.value(), EventEnvelope.class);

        // Continue the trace: the extracted remote context becomes the parent of this span.
        Span span = propagator.extract(record.headers(), KafkaTracingSupport.GETTER)
                .name("process " + Topics.PONG_EVENTS)
                .kind(Span.Kind.CONSUMER)
                .tag(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .tag(TracingAttributes.MESSAGING_OPERATION_TYPE, "process")
                .tag(TracingAttributes.MESSAGING_DESTINATION_NAME, Topics.PONG_EVENTS)
                .tag(TracingAttributes.MESSAGING_CONSUMER_GROUP_NAME, CONSUMER_GROUP)
                .tag(TracingAttributes.MESSAGING_KAFKA_OFFSET, String.valueOf(record.offset()))
                .tag(TracingAttributes.MESSAGING_DESTINATION_PARTITION_ID, String.valueOf(record.partition()))
                .tag(TracingAttributes.MESSAGING_MESSAGE_ID, env.eventId())
                .tag(TracingAttributes.APP_CAUSATION_ID, env.eventId())
                .tag(TracingAttributes.MESSAGE_TYPE, "event")
                .tag(TracingAttributes.EVENT_TYPE, env.eventType())
                .tag(TracingAttributes.DDD_BOUNDED_CONTEXT, Pong.BOUNDED_CONTEXT)
                .start();

        // Snapshot the saga id for the error log (scope/baggage-MDC is closed before the catch runs).
        String sagaId = null;
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            // Saga id arrived as Baggage in the extracted context; in scope it auto-populates the
            // "correlationId" MDC field. Stamp it on the span for queryability. traceId/spanId auto.
            sagaId = tracer.getBaggage(TracingAttributes.BAGGAGE_CORRELATION_ID).get();
            if (sagaId != null) {
                span.tag(TracingAttributes.MESSAGING_CONVERSATION_ID, sagaId);
            }
            MDC.put(TracingAttributes.MDC_CAUSATION_ID, env.eventId());

            if (!processed_event_ids.add(env.eventId())) {
                span.event(TracingAttributes.EVENT_DUPLICATE_DETECTED);
                span.tag(TracingAttributes.IDEMPOTENCY_OUTCOME, "skipped");
                return;
            }

            Map<String, Object> data = env.data();
            handlePongHandler.handle(new HandlePongCommand(
                    String.valueOf(data.get("pingId")),
                    String.valueOf(data.get("responder")),
                    env.eventId()));
        } catch (Exception e) {
            // Mark the span errored (Tempo) and log with trace_id + span_id + service for back-tracing.
            span.error(e);
            TraceContext ctx = span.context();
            log.error("Terminal pong handling failed service={} traceId={} spanId={} saga={} eventId={}: {}",
                    serviceName, ctx.traceId(), ctx.spanId(),
                    sagaId, env.eventId(), e.getMessage(), e);
            throw e;
        } finally {
            MDC.clear();
            span.end();
        }
    }
}
