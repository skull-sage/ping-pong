package com.pingpong.pongb.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.Topics;
import com.pingpong.common.TracingAttributes;
import com.pingpong.pongb.application.port.in.HandlePingCommand;
import com.pingpong.pongb.application.port.in.HandlePingUseCase;
import com.pingpong.pongb.domain.Pong;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * pong-b's fan-out subscriber. Independent consumer group, so it receives its own copy of every
 * {@code ping.events} record and starts its own new-root-linked trace (CR-2), correlated back to
 * the same saga via {@code correlationId}.
 */
@Component
public class PingEventListener {

    private static final String CONSUMER_GROUP = "service-pongB";

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
