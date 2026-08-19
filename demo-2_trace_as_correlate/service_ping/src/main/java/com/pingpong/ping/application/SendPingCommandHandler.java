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
 * CQRS command handler for {@link SendPingCommand} — the single entry point for BOTH the happy path
 * ({@code /api/ping}) and the failure-visualization path ({@code /api/ping/fail}).
 *
 * <p>Both go through the exact same infrastructure processing (repository + audit, each a Micrometer
 * {@code @Observed} span) and publish the same {@code PingCreated} event to the same
 * {@code ping.events} topic. The only difference is the {@code faulty} flag carried in the event:
 * when true, downstream service_pong logs an ERROR for it (see pong's HandlePingCommandHandler)
 * instead of responding onward to service_bang. Everything shares the one trace_id opened at the
 * REST controller.
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

    public void handle(SendPingCommand command) {
        String placePingCommandId = ReadableId.create(ORIGIN, "place-ping");
        String pingId = ReadableId.create(ORIGIN, "ping");

        Ping ping = Ping.create(pingId, command.note(), command.faulty());   // domain action #1 -> PingCreated

        // Local spans (Micrometer @Observed) that simulate a slow write + slow downstream call.
        // Identical for happy and failure paths — the failure only differs downstream, in pong.
        repository.save(ping);
        auditService.record(pingId, command.note());

        log.info("Starting ping-pong saga pingId={} faulty={}", pingId, command.faulty());
        for (Object event : ping.pull_events()) {
            if (event instanceof PingCreated created) {
                publisher.publish(Topics.PING_EVENTS, created.pingId(),
                        to_envelope(created, placePingCommandId));
            }
        }
    }

    private EventEnvelope to_envelope(PingCreated created, String causationId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pingId", created.pingId());
        data.put("note", created.note());
        data.put("faulty", created.faulty());   // rides the same event; pong branches on it
        data.put("createdAt", created.createdAt().toString());
        return EventEnvelope.builder()
                .event_id(ReadableId.create(ORIGIN, "ping-created"))
                .event_type("PingCreated")
                .event_version("1.0")
                .message_category("integration")
                .causation_id(causationId)
                .producer(ORIGIN)
                .data(data)
                .build();
    }
}
