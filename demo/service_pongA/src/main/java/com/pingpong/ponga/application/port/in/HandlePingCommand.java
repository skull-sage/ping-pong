package com.pingpong.ponga.application.port.in;

/**
 * Plain command mapped from the inbound event by the adapter. Carries the <b>business</b>
 * correlation ids (owned by the domain), never any OTel/trace-context types.
 *
 * @param inboundEventId the id of the ping event that caused this work (becomes the causationId)
 */
public record HandlePingCommand(String pingId, String note, String correlationId, String inboundEventId) {
}
