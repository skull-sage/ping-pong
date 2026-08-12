package com.pingpong.pong.application.command;

/**
 * Command emitted by the ping-event listener when a ping arrives.
 *
 * @param inboundEventId id of the ping event that caused this work (becomes the causationId)
 */
public record HandlePingCommand(String pingId, String note, String correlationId, String inboundEventId) {
}
