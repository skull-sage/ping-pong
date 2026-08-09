package com.pingpong.pongb.application;

import com.pingpong.common.EventEnvelope;
import com.pingpong.common.ReadableId;
import com.pingpong.common.Topics;
import com.pingpong.pongb.application.port.in.HandlePingCommand;
import com.pingpong.pongb.application.port.in.HandlePingUseCase;
import com.pingpong.pongb.application.port.out.EventPublisher;
import com.pingpong.pongb.domain.Pong;
import com.pingpong.pongb.domain.PongResponded;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Same shape as pong-a; keeps the saga correlationId static and chains causationId. */
@Service
public class HandlePingService implements HandlePingUseCase {

    private static final Logger log = LoggerFactory.getLogger(HandlePingService.class);
    private static final String ORIGIN = "service-pongB";

    private final EventPublisher eventPublisher;

    public HandlePingService(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void handle_ping(HandlePingCommand command) {
        PongResponded responded = Pong.respond_to(command.pingId());
        log.info("Responding to ping pingId={} corr={}", command.pingId(), command.correlationId());

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pingId", responded.pingId());
        data.put("responder", responded.responder());
        data.put("respondedAt", responded.respondedAt().toString());

        EventEnvelope envelope = EventEnvelope.builder()
                .event_id(ReadableId.create(ORIGIN, "pong-responded"))
                .event_type("PongResponded")
                .event_version("1.0")
                .message_category("integration")
                .correlation_id(command.correlationId())
                .causation_id(command.inboundEventId())
                .producer(ORIGIN)
                .data(data)
                .build();

        eventPublisher.publish(Topics.PONG_EVENTS, responded.pingId(), envelope);
    }
}
