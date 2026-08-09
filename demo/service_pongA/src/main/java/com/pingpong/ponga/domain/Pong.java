package com.pingpong.ponga.domain;

import java.time.Instant;

/** Pong aggregate (pure domain). Responding to a ping produces the {@link PongResponded} fact. */
public final class Pong {

    public static final String BOUNDED_CONTEXT = "pong-a";
    public static final String AGGREGATE_TYPE = "Pong";
    public static final String RESPONDER = "service-pongA";

    private Pong() {
    }

    public static PongResponded respond_to(String pingId) {
        return new PongResponded(pingId, RESPONDER, Instant.now());
    }
}
