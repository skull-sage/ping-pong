package com.pingpong.ping.presentation.web;

import com.pingpong.ping.application.SendPingCommandHandler;
import com.pingpong.ping.application.command.SendPingCommand;
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

    public PingController(SendPingCommandHandler sendPingHandler) {
        this.sendPingHandler = sendPingHandler;
    }

    @PostMapping("/ping")
    public ResponseEntity<Map<String, String>> ping(@RequestBody(required = false) PingRequest request) {
        String note = (request == null || request.note() == null) ? "ping" : request.note();
        String correlationId = sendPingHandler.handle(new SendPingCommand(note));
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "accepted", "correlationId", correlationId));
    }

    /** Optional JSON body: {"note": "..."}. */
    public record PingRequest(String note) {
    }
}
