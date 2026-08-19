package com.pingpong.pong.domain;

/**
 * Raised deliberately for the failure-visualization scenario (a ping that arrived on the normal
 * {@code ping.events} flow with {@code faulty=true}). It is thrown by {@code HandlePingCommandHandler},
 * logged at ERROR by {@code PingEventListener}, and recorded on the consumer span (status = error)
 * so it is observable in Loki and back-traceable in Tempo via the shared trace_id.
 */
public class FaultSimulationException extends RuntimeException {

    public FaultSimulationException(String message) {
        super(message);
    }
}
