package com.pingpong.ping.presentation.web;

import com.pingpong.ping.application.SendPingCommandHandler;
import com.pingpong.ping.application.TriggerFaultCommandHandler;
import com.pingpong.ping.application.command.SendPingCommand;
import com.pingpong.ping.application.command.TriggerFaultCommand;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Presentation layer — inbound HTTP adapter. On a REST request it emits a {@link SendPingCommand}
 * to the application command handler. Spring/Micrometer auto-instruments this as the SERVER span
 * that opens the trace.
 */
@RestController
@RequestMapping("/api")
public class PingController {

    private final SendPingCommandHandler sendPingHandler;
    private final TriggerFaultCommandHandler triggerFaultHandler;

    public PingController(SendPingCommandHandler sendPingHandler,
                          TriggerFaultCommandHandler triggerFaultHandler) {
        this.sendPingHandler = sendPingHandler;
        this.triggerFaultHandler = triggerFaultHandler;
    }

    @PostMapping("/ping")
    public ResponseEntity<Map<String, String>> ping(@RequestBody(required = false) PingRequest request) {
        String note = (request == null || request.note() == null) ? "ping" : request.note();
        String correlationId = sendPingHandler.handle(new SendPingCommand(note));
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "accepted", "correlationId", correlationId));
    }

    /**
     * CR-2 failure pipeline. Emits a {@link TriggerFaultCommand} that publishes a fault-request
     * event to {@code ping.faults}; service_pong consumes it and deliberately raises a logged
     * exception. The returned {@code correlationId} lets you back-trace that error in Loki/Tempo.
     */
    @PostMapping("/ping/fail")
    public ResponseEntity<Map<String, String>> fail(@RequestBody(required = false) FaultRequest request) {
        String reason = (request == null || request.reason() == null)
                ? "simulated-downstream-failure" : request.reason();
        String correlationId = triggerFaultHandler.handle(new TriggerFaultCommand(reason));
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "fault-dispatched", "correlationId", correlationId));
    }

    /** Optional JSON body: {"note": "..."}. */
    public record PingRequest(String note) {
    }

    /** Optional JSON body: {"reason": "..."}. */
    public record FaultRequest(String reason) {
    }
}
