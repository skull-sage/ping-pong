package com.pingpong.ponga.application.port.in;

/** Inbound port: react to an incoming ping and emit a pong. */
public interface HandlePingUseCase {

    void handle_ping(HandlePingCommand command);
}
