package com.pingpong.ping.application.port.in;

/** Inbound port for the fan-in leg: a pong came back and closes (part of) the saga. */
public interface RecordPongUseCase {

    void record_pong(RecordPongCommand command);
}
