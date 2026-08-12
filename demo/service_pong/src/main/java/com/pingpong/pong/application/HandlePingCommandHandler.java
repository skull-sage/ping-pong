package com.pingpong.pong.application;

import com.pingpong.common.EventEnvelope;
import com.pingpong.common.ReadableId;
import com.pingpong.common.Topics;
import com.pingpong.pong.application.command.HandlePingCommand;
import com.pingpong.pong.domain.Pong;
import com.pingpong.pong.domain.PongResponded;
import com.pingpong.pong.infrastructure.messaging.KafkaEventPublisher;
import com.pingpong.pong.infrastructure.persistence.PongRepository;
import com.pingpong.pong.infrastructure.service.PongProcessingService;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CQRS command handler for {@link HandlePingCommand} — the span-tracing handler. It drives the
 * aggregate and calls a repository + a processing service that are {@code @WithSpan}-annotated and
 * sleep, so the internal performance hazard shows up as child spans of this consumer's trace.
 * The saga {@code correlationId} is kept static; {@code causationId} is the inbound ping event id.
 */
@Component
public class HandlePingCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(HandlePingCommandHandler.class);
    private static final String ORIGIN = "service-pong";

    private final PongRepository repository;
    private final PongProcessingService processingService;
    private final KafkaEventPublisher publisher;

    public HandlePingCommandHandler(PongRepository repository,
                                    PongProcessingService processingService,
                                    KafkaEventPublisher publisher) {
        this.repository = repository;
        this.processingService = processingService;
        this.publisher = publisher;
    }

    public void handle(HandlePingCommand command) {
        Pong pong = Pong.receive(command.pingId());     // domain action #1 -> PingReceived

        repository.save(pong);                           // @WithSpan + sleep
        processingService.process(command.pingId());     // @WithSpan + sleep

        pong.respond();                                  // domain action #2 -> PongResponded
        log.info("Responding to ping pingId={} corr={}", command.pingId(), command.correlationId());

        for (Object event : pong.pull_events()) {
            if (event instanceof PongResponded responded) {
                publisher.publish(Topics.PONG_EVENTS, responded.pingId(),
                        to_envelope(responded, command));
            }
        }
    }

    private EventEnvelope to_envelope(PongResponded responded, HandlePingCommand command) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pingId", responded.pingId());
        data.put("responder", responded.responder());
        data.put("respondedAt", responded.respondedAt().toString());
        return EventEnvelope.builder()
                .event_id(ReadableId.create(ORIGIN, "pong-responded"))
                .event_type("PongResponded")
                .event_version("1.0")
                .message_category("integration")
                .correlation_id(command.correlationId())   // kept static across the saga
                .causation_id(command.inboundEventId())     // the ping event caused this pong
                .producer(ORIGIN)
                .data(data)
                .build();
    }
}
