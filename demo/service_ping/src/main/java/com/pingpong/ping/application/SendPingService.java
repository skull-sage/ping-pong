package com.pingpong.ping.application;

import com.pingpong.common.EventEnvelope;
import com.pingpong.common.ReadableId;
import com.pingpong.common.Topics;
import com.pingpong.ping.application.port.in.SendPingCommand;
import com.pingpong.ping.application.port.in.SendPingUseCase;
import com.pingpong.ping.application.port.out.EventPublisher;
import com.pingpong.ping.domain.Ping;
import com.pingpong.ping.domain.PingCreated;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Application service. Pure orchestration: it creates the aggregate, mints the business
 * correlation ids, builds the integration-event envelope, and hands it to the outbound port.
 * It has no idea a broker or OpenTelemetry exists.
 */
@Service
public class SendPingService implements SendPingUseCase {

    private static final Logger log = LoggerFactory.getLogger(SendPingService.class);
    private static final String ORIGIN = "service-ping";

    private final EventPublisher eventPublisher;

    public SendPingService(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public String send_ping(SendPingCommand command) {
        // Mint the saga id (correlationId) and the id of the command that starts the flow.
        String correlationId = ReadableId.create(ORIGIN, "ping-saga");
        String placePingCommandId = ReadableId.create(ORIGIN, "place-ping");

        String pingId = ReadableId.create(ORIGIN, "ping");
        PingCreated created = Ping.create(pingId, command.note());

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pingId", created.pingId());
        data.put("note", created.note());
        data.put("createdAt", created.createdAt().toString());

        EventEnvelope envelope = EventEnvelope.builder()
                .event_id(ReadableId.create(ORIGIN, "ping-created"))
                .event_type("PingCreated")
                .event_version("1.0")
                .message_category("integration")
                .correlation_id(correlationId)
                .causation_id(placePingCommandId) // the command that caused this event
                .producer(ORIGIN)
                .data(data)
                .build();

        log.info("Starting ping-pong saga corr={} pingId={}", correlationId, pingId);
        eventPublisher.publish(Topics.PING_EVENTS, created.pingId(), envelope);
        return correlationId;
    }
}
