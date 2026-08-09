package com.pingpong.ping.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingpong.common.EventEnvelope;
import com.pingpong.common.TracingAttributes;
import com.pingpong.common.Topics;
import com.pingpong.ping.application.port.in.RecordPongCommand;
import com.pingpong.ping.application.port.in.RecordPongUseCase;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Inbound adapter for the fan-in leg. A {@code pong.events} message is an <b>event</b>, so we start
 * a <b>new root trace linked</b> to the producer's publish span (CR-2, §7.6 / §15.2) rather than
 * extending its trace. {@code correlationId} is kept; {@code causationId} is promoted to the
 * inbound event id. Both are re-stamped into the log MDC for three-pillar correlation (CR-4).
 */
@Component
public class PongEventListener {

    private final RecordPongUseCase recordPong;
    private final ObjectMapper mapper;
    private final OpenTelemetry openTelemetry;

    public PongEventListener(RecordPongUseCase recordPong,
                             ObjectMapper mapper,
                             OpenTelemetry openTelemetry) {
        this.recordPong = recordPong;
        this.mapper = mapper;
        this.openTelemetry = openTelemetry;
    }

    @KafkaListener(topics = Topics.PONG_EVENTS, groupId = "service-ping")
    public void on_pong(ConsumerRecord<String, String> record) throws Exception {
        // 1) Rebuild the producer's context to LINK (not parent) back to its publish span.
        Context producerCtx = openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), record.headers(), KafkaTracingSupport.GETTER);
        SpanContext producerSpan = Span.fromContext(producerCtx).getSpanContext();

        EventEnvelope env = mapper.readValue(record.value(), EventEnvelope.class);

        Span span = openTelemetry.getTracer("service-ping")
                .spanBuilder("process " + Topics.PONG_EVENTS)
                .setNoParent()                 // new root trace_id (async fan-out)
                .addLink(producerSpan)         // keep the connection to the publish span
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(TracingAttributes.MESSAGING_SYSTEM, "kafka")
                .setAttribute(TracingAttributes.MESSAGING_OPERATION_TYPE, "process")
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_NAME, Topics.PONG_EVENTS)
                .setAttribute(TracingAttributes.MESSAGING_CONSUMER_GROUP_NAME, "service-ping")
                .setAttribute(TracingAttributes.MESSAGING_KAFKA_OFFSET, record.offset())
                .setAttribute(TracingAttributes.MESSAGING_DESTINATION_PARTITION_ID, record.partition())
                .setAttribute(TracingAttributes.MESSAGING_MESSAGE_ID, env.eventId())
                .setAttribute(TracingAttributes.MESSAGING_CONVERSATION_ID, env.correlationId()) // kept
                .setAttribute(TracingAttributes.APP_CAUSATION_ID, env.eventId())                // promoted
                .setAttribute(TracingAttributes.MESSAGE_TYPE, "event")
                .setAttribute(TracingAttributes.EVENT_TYPE, env.eventType())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            MDC.put(TracingAttributes.MDC_CORRELATION_ID, env.correlationId());
            MDC.put(TracingAttributes.MDC_CAUSATION_ID, env.eventId());

            Map<String, Object> data = env.data();
            recordPong.record_pong(new RecordPongCommand(
                    env.producer(),
                    String.valueOf(data.get("pingId")),
                    env.correlationId()));
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
