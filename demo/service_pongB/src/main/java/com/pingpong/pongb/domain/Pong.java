package com.pingpong.pongb.domain;

import java.time.Instant;

/** Pong aggregate (pure domain) for the pong-b context. */
public final class Pong {

    public static final String BOUNDED_CONTEXT = "pong-b";
    public static final String AGGREGATE_TYPE = "Pong";
    public static final String RESPONDER = "service-pongB";

    private Pong() {
    }

    public static PongResponded respond_to(String pingId) {
        return new PongResponded(pingId, RESPONDER, Instant.now());
    }
}
