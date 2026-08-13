package com.pingpong.pong.application;

import com.pingpong.pong.application.command.HandleFaultCommand;
import com.pingpong.pong.domain.FaultSimulationException;
import com.pingpong.pong.infrastructure.service.PongProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CQRS command handler for {@link HandleFaultCommand} — the failing branch of the CR-2 pipeline.
 * It does a little {@code @WithSpan} work (so the trace has depth) and then deliberately throws
 * {@link FaultSimulationException}. The listener catches, logs at ERROR, and marks the span errored
 * so the failure is observable in Loki and back-traceable in Tempo.
 */
@Component
public class HandleFaultCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(HandleFaultCommandHandler.class);

    private final PongProcessingService processingService;

    public HandleFaultCommandHandler(PongProcessingService processingService) {
        this.processingService = processingService;
    }

    public void handle(HandleFaultCommand command) {
        processingService.process(command.pingId());   // @WithSpan + sleep, so the errored span has a child
        log.warn("About to raise a simulated fault pingId={} corr={} reason={}",
                command.pingId(), command.correlationId(), command.reason());
        throw new FaultSimulationException(
                "Simulated downstream failure for pingId=" + command.pingId()
                        + " reason=" + command.reason());
    }
}
