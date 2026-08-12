package com.pingpong.bang.application;

import com.pingpong.bang.domain.Pong;
import com.pingpong.bang.application.command.HandlePongCommand;
import com.pingpong.bang.infrastructure.persistence.PongRepository;
import com.pingpong.bang.infrastructure.service.PongProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CQRS command handler for {@link HandlePongCommand} — the terminal, span-tracing handler of the
 * ping → pong → bang chain. It drives the aggregate and calls the {@code @WithSpan} repository +
 * processing service (which sleep) so the internal performance hazard shows up as child spans of
 * bang's consumer trace. Being the last link, it publishes nothing downstream.
 */
@Component
public class HandlePongCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(HandlePongCommandHandler.class);

    private final PongRepository repository;
    private final PongProcessingService processingService;

    public HandlePongCommandHandler(PongRepository repository,
                                    PongProcessingService processingService) {
        this.repository = repository;
        this.processingService = processingService;
    }

    public void handle(HandlePongCommand command) {
        Pong pong = Pong.receive(command.pingId(), command.responder());  // action #1 -> PongReceived

        repository.save(pong);                          // @WithSpan + sleep
        processingService.process(command.pingId());    // @WithSpan + sleep

        pong.complete();                                // action #2 -> ChainCompleted
        log.info("Chain completed for pingId={} respondedBy={} corr={} (events raised={})",
                command.pingId(), command.responder(), command.correlationId(),
                pong.pull_events().size());
    }
}
