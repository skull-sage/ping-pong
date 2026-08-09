package com.pingpong.ping.infrastructure.web;

import com.pingpong.ping.application.port.in.SendPingCommand;
import com.pingpong.ping.application.port.in.SendPingUseCase;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inbound HTTP adapter. Spring/Micrometer auto-instruments this as the SERVER span that opens the
 * trace; the PRODUCER span created downstream by the publisher becomes its child (same trace).
 */
@RestController
@RequestMapping("/api")
public class PingController {

    private final SendPingUseCase sendPing;

    public PingController(SendPingUseCase sendPing) {
        this.sendPing = sendPing;
    }

    @PostMapping("/ping")
    public ResponseEntity<Map<String, String>> ping(@RequestBody(required = false) PingRequest request) {
        String note = request == null || request.note() == null ? "ping" : request.note();
        String correlationId = sendPing.send_ping(new SendPingCommand(note));
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "accepted", "correlationId", correlationId));
    }

    /** Optional JSON body: {"note": "..."}. */
    public record PingRequest(String note) {
    }
}
