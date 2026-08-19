package com.pingpong.bang.application.command;

/**
 * Command emitted by the pong-event listener when a pong arrives (the ping → pong → bang chain).
 *
 * <p>No correlation id is passed: correlation is the trace_id and the readable saga id rides in
 * Baggage. {@code inboundEventId} is domain causation metadata.
 *
 * @param inboundEventId id of the pong event that caused this work (becomes the causationId)
 */
public record HandlePongCommand(String pingId, String responder, String inboundEventId) {
}
