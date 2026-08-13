package com.pingpong.ping.application;

import com.pingpong.common.EventEnvelope;
import com.pingpong.common.ReadableId;
import com.pingpong.common.Topics;
import com.pingpong.ping.application.command.TriggerFaultCommand;
import com.pingpong.ping.domain.FaultRequested;
import com.pingpong.ping.domain.Ping;
import com.pingpong.ping.infrastructure.messaging.KafkaEventPublisher;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CQRS command handler for {@link TriggerFaultCommand} — the entry point of the CR-2 failure
 * pipeline. It drives the {@link Ping} aggregate to raise a {@link FaultRequested} event and
 * publishes it to {@code ping.faults}. service_pong consumes that event and deliberately throws,
 * so the error is logged and back-traceable to this publish span via the shared {@code correlationId}.
 */
@Component
public class TriggerFaultCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(TriggerFaultCommandHandler.class);
    private static final String ORIGIN = "service-ping";

    private final KafkaEventPublisher publisher;

    public TriggerFaultCommandHandler(KafkaEventPublisher publisher) {
        this.publisher = publisher;
    }

    /** @return the saga's correlationId (returned to the caller so the failure can be traced). */
    public String handle(TriggerFaultCommand command) {
        String correlationId = ReadableId.create(ORIGIN, "fault-saga");
        String triggerFaultCommandId = ReadableId.create(ORIGIN, "trigger-fault");
        String pingId = ReadableId.create(ORIGIN, "ping");

        Ping ping = Ping.rehydrate(pingId);
        ping.request_fault(command.reason());            // domain action #3 -> FaultRequested

        log.info("Starting fault-injection saga corr={} pingId={} reason={}",
                correlationId, pingId, command.reason());

        for (Object event : ping.pull_events()) {
            if (event instanceof FaultRequested requested) {
                publisher.publish(Topics.PING_FAULTS, requested.pingId(),
                        to_envelope(requested, correlationId, triggerFaultCommandId));
            }
        }
        return correlationId;
    }

    private EventEnvelope to_envelope(FaultRequested requested, String correlationId, String causationId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pingId", requested.pingId());
        data.put("reason", requested.reason());
        data.put("requestedAt", requested.requestedAt().toString());
        return EventEnvelope.builder()
                .event_id(ReadableId.create(ORIGIN, "fault-requested"))
                .event_type("FaultRequested")
                .event_version("1.0")
                .message_category("integration")
                .correlation_id(correlationId)
                .causation_id(causationId)
                .producer(ORIGIN)
                .data(data)
                .build();
    }
}
