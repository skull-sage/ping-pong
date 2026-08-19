package com.pingpong.pong.presentation.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.Topics;
import com.pingpong.common.TracingAttributes;
import com.pingpong.pong.application.HandlePingCommandHandler;
import com.pingpong.pong.application.command.HandlePingCommand;
import com.pingpong.pong.domain.FaultSimulationException;
import com.pingpong.pong.domain.Pong;
import com.pingpong.pong.infrastructure.messaging.KafkaTracingSupport;
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
 * Presentation layer — inbound event adapter for the chain middle. Consumes {@code ping.events}
 * (both happy-path and failure-visualization pings arrive here — they differ only by the
 * {@code faulty} flag on the event).
 *
 * <p><b>Single-trace_id:</b> we {@link Propagator#extract extract} the W3C context service_ping
 * injected and use it as the parent of this CONSUMER span, so it CONTINUES the one trace that began
 * at the REST controller.
 *
 * <p><b>Failure visualization:</b> when the handler throws {@link FaultSimulationException} (the
 * faulty branch), we mark the span errored and log it at ERROR with the trace_id / span_id / service
 * name, then swallow it (no rethrow) so the demo produces exactly one clean error per faulty request
 * with no Kafka redelivery storm. Any OTHER exception is a real error and is rethrown.
 */
@Component
public class PingEventListener {

    private static final Logger log = LoggerFactory.getLogger(PingEventListener.class);
    private static final String CONSUMER_GROUP = "service-pong";

    private final Set<String> processed_event_ids = ConcurrentHashMap.newKeySet();

    private final HandlePingCommandHandler handlePingHandler;
    private final ObjectMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;
    private final String serviceName;

    public PingEventListener(HandlePingCommandHandler handlePingHandler,
                             ObjectMapper mapper,
                             Tracer tracer,
                             Propagator propagator,
                             @Value("${spring.application.name}") String serviceName) {
        this.handlePingHandler = handlePingHandler;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
        this.serviceName = serviceName;
    }

    @KafkaListener(topics = Topics.PING_EVENTS, groupId = CONSUMER_GROUP)
    public void on_ping(ConsumerRecord<String, String> record) throws Exception {
        EventEnvelope env = mapper.readValue(record.value(), EventEnvelope.class);

        // extract(...) returns a Span.Builder whose PARENT is the remote context from the headers,
        // so start() continues the SAME trace (same trace_id) that started on service_ping.
        Span span = propagator.extract(record.headers(), KafkaTracingSupport.GETTER)
                .name("process " + Topics.PING_EVENTS)
                .kind(Span.Kind.CONSUMER)
                .tag(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .tag(TracingAttributes.MESSAGING_OPERATION_TYPE, "process")
                .tag(TracingAttributes.MESSAGING_DESTINATION_NAME, Topics.PING_EVENTS)
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
            handlePingHandler.handle(new HandlePingCommand(
                    String.valueOf(data.get("pingId")),
                    String.valueOf(data.get("note")),
                    Boolean.TRUE.equals(data.get("faulty")),
                    env.eventId()));
        } catch (FaultSimulationException e) {
            // Injected failure scenario: mark the span errored (Tempo) and log with trace_id +
            // span_id + service (Loki). Swallowed on purpose — one clean error, no redelivery storm.
            span.error(e);
            TraceContext ctx = span.context();
            log.error("Ping processing failed (simulated) service={} traceId={} spanId={} saga={} eventId={}: {}",
                    serviceName, ctx.traceId(), ctx.spanId(), sagaId, env.eventId(), e.getMessage());
        } catch (Exception e) {
            // A real, unexpected error — record it and rethrow so Kafka can retry.
            span.error(e);
            throw e;
        } finally {
            MDC.clear();
            span.end();
        }
    }
}
