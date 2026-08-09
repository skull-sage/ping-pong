package com.pingpong.ping.application.port.in;

/** Plain command for the fan-in leg. */
public record RecordPongCommand(String responder, String pingId, String correlationId) {
}
