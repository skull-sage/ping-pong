package com.pingpong.bang.presentation.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.bang.application.HandlePongCommandHandler;
import com.pingpong.bang.application.command.HandlePongCommand;
import com.pingpong.bang.domain.Pong;
import com.pingpong.bang.infrastructure.messaging.KafkaTracingSupport;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.Topics;
import com.pingpong.common.TracingAttributes;
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
 * Presentation layer — inbound event adapter for the terminal link of the ping → pong → bang chain.
 * It consumes {@code pong.events} (the {@code PongResponded} fact from service_pong). Because that
 * is an event, it starts a <b>new root trace linked</b> to the pong publish span (CR-2), dedups on
 * the message id (RC-4), then emits a {@link HandlePongCommand} to the application handler.
 */
@Component
public class PongEventListener {

    private static final String CONSUMER_GROUP = "service-bang";

    private final Set<String> processed_event_ids = ConcurrentHashMap.newKeySet();

    private final HandlePongCommandHandler handlePongHandler;
    private final ObjectMapper mapper;
    private final OpenTelemetry openTelemetry;

    public PongEventListener(HandlePongCommandHandler handlePongHandler,
                             ObjectMapper mapper,
                             OpenTelemetry openTelemetry) {
        this.handlePongHandler = handlePongHandler;
        this.mapper = mapper;
        this.openTelemetry = openTelemetry;
    }

    @KafkaListener(topics = Topics.PONG_EVENTS, groupId = CONSUMER_GROUP)
    public void on_pong(ConsumerRecord<String, String> record) throws Exception {
        Context producerCtx = openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), record.headers(), KafkaTracingSupport.GETTER);
        SpanContext producerSpan = Span.fromContext(producerCtx).getSpanContext();

        EventEnvelope env = mapper.readValue(record.value(), EventEnvelope.class);

        Span span = openTelemetry.getTracer(Pong.CONSUMER)
                .spanBuilder("process " + Topics.PONG_EVENTS)
                .setNoParent()
                .addLink(producerSpan)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .setAttribute(TracingAttributes.MESSAGING_OPERATION_TYPE, "process")
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_NAME, Topics.PONG_EVENTS)
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
            handlePongHandler.handle(new HandlePongCommand(
                    String.valueOf(data.get("pingId")),
                    String.valueOf(data.get("responder")),
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
