package com.pingpong.pong.application.command;

/**
 * Command emitted by the ping-event listener when a ping arrives.
 *
 * <p>No correlation id is passed: correlation is the trace_id (propagated in context) and the
 * readable saga id rides in Baggage. {@code inboundEventId} is domain causation metadata.
 *
 * @param faulty         when true this is the failure-visualization scenario — pong logs an ERROR
 *                       for it instead of responding onward to service_bang
 * @param inboundEventId id of the ping event that caused this work (becomes the causationId)
 */
public record HandlePingCommand(String pingId, String note, boolean faulty, String inboundEventId) {
}
