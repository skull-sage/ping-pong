package com.pingpong.ponga.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.Topics;
import com.pingpong.common.TracingAttributes;
import com.pingpong.ponga.application.port.in.HandlePingCommand;
import com.pingpong.ponga.application.port.in.HandlePingUseCase;
import com.pingpong.ponga.domain.Pong;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Inbound adapter for the event fan-out (§7.6 / §15.2). Each {@code ping.events} record starts a
 * <b>new root trace linked</b> to the publish span (CR-2), keeps {@code correlationId}, promotes
 * {@code causationId} to the inbound event id, and dedups on {@code messaging.message.id} (RC-4).
 */
@Component
public class PingEventListener {

    private static final String CONSUMER_GROUP = "service-pongA";

    // Idempotency store (RC-4). In-memory is fine for the demo; production uses an inbox table.
    private final Set<String> processed_event_ids = ConcurrentHashMap.newKeySet();

    private final HandlePingUseCase handlePing;
    private final ObjectMapper mapper;
    private final OpenTelemetry openTelemetry;

    public PingEventListener(HandlePingUseCase handlePing,
                             ObjectMapper mapper,
                             OpenTelemetry openTelemetry) {
        this.handlePing = handlePing;
        this.mapper = mapper;
        this.openTelemetry = openTelemetry;
    }

    @KafkaListener(topics = Topics.PING_EVENTS, groupId = CONSUMER_GROUP)
    public void on_ping(ConsumerRecord<String, String> record) throws Exception {
        Context producerCtx = openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), record.headers(), KafkaTracingSupport.GETTER);
        SpanContext producerSpan = Span.fromContext(producerCtx).getSpanContext();

        EventEnvelope env = mapper.readValue(record.value(), EventEnvelope.class);

        Span span = openTelemetry.getTracer(Pong.RESPONDER)
                .spanBuilder("process " + Topics.PING_EVENTS)
                .setNoParent()
                .addLink(producerSpan)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .setAttribute(TracingAttributes.MESSAGING_OPERATION_TYPE, "process")
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_NAME, Topics.PING_EVENTS)
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

            // Duplicate detection (RC-4): record why a repeat is skipped so a replay is not
            // mistaken for lost work.
            if (!processed_event_ids.add(env.eventId())) {
                span.addEvent(TracingAttributes.EVENT_DUPLICATE_DETECTED, Attributes.builder()
                        .put(TracingAttributes.MESSAGING_MESSAGE_ID, env.eventId())
                        .put(TracingAttributes.IDEMPOTENCY_OUTCOME, "skipped")
                        .build());
                return;
            }

            Map<String, Object> data = env.data();
            handlePing.handle_ping(new HandlePingCommand(
                    String.valueOf(data.get("pingId")),
                    String.valueOf(data.get("note")),
                    env.correlationId(),
                    env.eventId()));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            MDC.clear();
            span.end();
        }
    }
}
