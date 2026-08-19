package com.pingpong.ping.presentation.web;

import com.pingpong.common.ReadableId;
import com.pingpong.common.TracingAttributes;
import com.pingpong.ping.application.SendPingCommandHandler;
import com.pingpong.ping.application.command.SendPingCommand;
import io.micrometer.tracing.BaggageInScope;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Presentation layer — inbound HTTP adapter and <b>the origin of the trace</b>.
 *
 * <p>Two endpoints, ONE pipeline:
 * <ul>
 *   <li>{@code POST /api/ping}      — happy path: processing -> {@code ping.events} -> pong -> bang.</li>
 *   <li>{@code POST /api/ping/fail} — failure-visualization: the <b>same</b> processing and the
 *       <b>same</b> {@code ping.events} flow, but the event is flagged {@code faulty=true} so
 *       service_pong logs an ERROR for it (visible in Loki, back-traceable in Tempo). It does not
 *       continue to service_bang.</li>
 * </ul>
 *
 * <p><b>Where the single trace_id is born:</b> Spring Boot + Micrometer auto-instrument every inbound
 * request as a SERVER span (the root of a new trace). That trace_id is the one correlator for the
 * whole lifecycle. A human-readable saga id is set in Baggage (key {@code correlationId}) and
 * propagates in the trace context — including across Kafka — so it appears in every service's logs.
 * Both endpoints return {@code 202 Accepted}; the failure surfaces asynchronously in pong.
 */
@RestController
@RequestMapping("/api")
public class PingController {

    private static final String ORIGIN = "service-ping";

    private final SendPingCommandHandler sendPingHandler;
    private final Tracer tracer;

    public PingController(SendPingCommandHandler sendPingHandler, Tracer tracer) {
        this.sendPingHandler = sendPingHandler;
        this.tracer = tracer;
    }

    @PostMapping("/ping")
    public ResponseEntity<Map<String, String>> ping(@RequestBody(required = false) PingRequest request) {
        String note = (request == null || request.note() == null) ? "ping" : request.note();
        return dispatch(new SendPingCommand(note, false), "ping-saga", "accepted");
    }

    /**
     * Failure-visualization path. Emits the same {@link SendPingCommand} but flagged faulty, so the
     * identical event flow reaches service_pong which logs an ERROR. The returned {@code traceId}
     * back-traces that downstream error to this exact request in Tempo/Loki.
     */
    @PostMapping("/ping/fail")
    public ResponseEntity<Map<String, String>> fail(@RequestBody(required = false) FaultRequest request) {
        String reason = (request == null || request.reason() == null)
                ? "simulated-downstream-failure" : request.reason();
        return dispatch(new SendPingCommand(reason, true), "fault-saga", "fault-dispatched");
    }

    /** Runs the handler inside a Baggage scope (saga id) and returns the trace_id to the caller. */
    private ResponseEntity<Map<String, String>> dispatch(SendPingCommand command, String sagaKind, String status) {
        String sagaId = ReadableId.create(ORIGIN, sagaKind);
        // Open the Baggage scope BEFORE calling the handler so it is active when the publish span
        // injects the trace context (with baggage) into the Kafka headers -> propagates downstream.
        try (BaggageInScope ignored =
                     tracer.createBaggageInScope(TracingAttributes.BAGGAGE_CORRELATION_ID, sagaId)) {
            sendPingHandler.handle(command);
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "status", status,
                        "traceId", currentTraceId(),      // the single lifecycle correlator
                        "correlationId", sagaId));         // readable business saga id (Baggage)
    }

    /** Reads the trace_id of the active SERVER span (the trace opened by this request). */
    private String currentTraceId() {
        Span span = tracer.currentSpan();
        return span == null ? "unknown" : span.context().traceId();
    }

    /** Optional JSON body: {"note": "..."}. */
    public record PingRequest(String note) {
    }

    /** Optional JSON body: {"reason": "..."}. */
    public record FaultRequest(String reason) {
    }
}
