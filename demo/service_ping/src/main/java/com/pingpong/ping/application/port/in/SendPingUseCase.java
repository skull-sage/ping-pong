package com.pingpong.ping.application.port.in;

/** Inbound port: start one ping-pong saga. Driven by the HTTP adapter. */
public interface SendPingUseCase {

    /** @return the correlationId (saga id) of the started flow. */
    String send_ping(SendPingCommand command);
}
