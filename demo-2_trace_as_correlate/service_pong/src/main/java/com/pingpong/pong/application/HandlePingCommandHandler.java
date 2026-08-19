package com.pingpong.pong.application;

import com.pingpong.common.EventEnvelope;
import com.pingpong.common.ReadableId;
import com.pingpong.common.Topics;
import com.pingpong.pong.application.command.HandlePingCommand;
import com.pingpong.pong.domain.FaultSimulationException;
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
 * CQRS command handler for {@link HandlePingCommand} — the span-tracing handler for BOTH the happy
 * path and the failure-visualization path (they arrive on the same {@code ping.events} topic).
 *
 * <p>It always does the same {@code @Observed} work (repository + processing, which sleep) so the
 * internal spans show up under the continued trace (same trace_id from service_ping). Then it branches:
 * <ul>
 *   <li><b>normal:</b> respond and publish {@code PongResponded} to {@code pong.events} (-> service_bang).</li>
 *   <li><b>faulty:</b> throw {@link FaultSimulationException}. The listener catches it, marks the
 *       consumer span errored, and logs it at ERROR (for visualization / back-tracing). Nothing is
 *       published onward, so the chain deliberately stops at the pong error.</li>
 * </ul>
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

        repository.save(pong);                           // @Observed + sleep
        processingService.process(command.pingId());     // @Observed + sleep

        // Failure-visualization: same request + same event flow, but this one errors out at pong.
        if (command.faulty()) {
            throw new FaultSimulationException(
                    "Simulated downstream failure for pingId=" + command.pingId()
                            + " note=" + command.note());
        }

        pong.respond();                                  // domain action #2 -> PongResponded
        log.info("Responding to ping pingId={}", command.pingId());   // saga id + traceId via MDC

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
                .causation_id(command.inboundEventId())     // the ping event caused this pong
                .producer(ORIGIN)
                .data(data)
                .build();
    }
}
