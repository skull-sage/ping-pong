package com.pingpong.pong.domain;

/**
 * Raised deliberately by the CR-2 failure pipeline to exercise the error path. It is thrown by
 * {@code HandleFaultCommandHandler}, logged at ERROR by the listener, and recorded on the consumer
 * span (status = error) so it is observable in Loki and back-traceable in Tempo.
 */
public class FaultSimulationException extends RuntimeException {

    public FaultSimulationException(String message) {
        super(message);
    }
}
