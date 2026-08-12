package com.pingpong.bang.application.command;

/**
 * Command emitted by the pong-event listener when a pong arrives (the ping → pong → bang chain).
 *
 * @param inboundEventId id of the pong event that caused this work (becomes the causationId)
 */
public record HandlePongCommand(String pingId, String responder, String correlationId, String inboundEventId) {
}
