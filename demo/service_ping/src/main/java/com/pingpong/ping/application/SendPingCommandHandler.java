package com.pingpong.ping.application;

import com.pingpong.common.EventEnvelope;
import com.pingpong.common.ReadableId;
import com.pingpong.common.Topics;
import com.pingpong.ping.application.command.SendPingCommand;
import com.pingpong.ping.domain.Ping;
import com.pingpong.ping.domain.PingCreated;
import com.pingpong.ping.infrastructure.messaging.KafkaEventPublisher;
import com.pingpong.ping.infrastructure.persistence.PingRepository;
import com.pingpong.ping.infrastructure.service.PingAuditService;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CQRS command handler for {@link SendPingCommand}. This is the <b>span-tracing</b> handler: it
 * drives the aggregate and then calls a repository and a downstream service that are both annotated
 * with {@code @WithSpan} and deliberately sleep, so the trace shows the internal performance
 * hazard as child spans under the HTTP + publish spans.
 */
@Component
public class SendPingCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(SendPingCommandHandler.class);
    private static final String ORIGIN = "service-ping";

    private final PingRepository repository;
    private final PingAuditService auditService;
    private final KafkaEventPublisher publisher;

    public SendPingCommandHandler(PingRepository repository,
                                  PingAuditService auditService,
                                  KafkaEventPublisher publisher) {
                                    
        this.repository = repository;
        this.auditService = auditService;
        this.publisher = publisher;
    }

    /** @return the saga's correlationId. */
    public String handle(SendPingCommand command) {
        String correlationId = ReadableId.create(ORIGIN, "ping-saga");
        String placePingCommandId = ReadableId.create(ORIGIN, "place-ping");
        String pingId = ReadableId.create(ORIGIN, "ping");

        Ping ping = Ping.create(pingId, command.note());   // domain action #1 -> PingCreated

        // Local spans (@WithSpan) that simulate a slow write + slow downstream call.
        repository.save(ping);
        auditService.record(pingId, command.note());

        log.info("Starting ping-pong saga corr={} pingId={}", correlationId, pingId);
        for (Object event : ping.pull_events()) {
            if (event instanceof PingCreated created) {
                publisher.publish(Topics.PING_EVENTS, created.pingId(),
                        to_envelope(created, correlationId, placePingCommandId));
            }
        }
        return correlationId;
    }

    private EventEnvelope to_envelope(PingCreated created, String correlationId, String causationId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pingId", created.pingId());
        data.put("note", created.note());
        data.put("createdAt", created.createdAt().toString());
        return EventEnvelope.builder()
                .event_id(ReadableId.create(ORIGIN, "ping-created"))
                .event_type("PingCreated")
                .event_version("1.0")
                .message_category("integration")
                .correlation_id(correlationId)
                .causation_id(causationId)
                .producer(ORIGIN)
                .data(data)
                .build();
    }
}
