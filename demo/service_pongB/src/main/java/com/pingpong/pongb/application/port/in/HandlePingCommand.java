package com.pingpong.pongb.application.port.in;

/**
 * Plain command mapped from the inbound event by the adapter.
 *
 * @param inboundEventId the id of the ping event that caused this work (becomes the causationId)
 */
public record HandlePingCommand(String pingId, String note, String correlationId, String inboundEventId) {
}
