package com.pingpong.pong.application.command;

/**
 * Command emitted by the fault-event listener when a fault-request arrives (CR-2 failure pipeline).
 *
 * @param inboundEventId id of the fault event that caused this work (becomes the causationId)
 */
public record HandleFaultCommand(String pingId, String reason, String correlationId, String inboundEventId) {
}
