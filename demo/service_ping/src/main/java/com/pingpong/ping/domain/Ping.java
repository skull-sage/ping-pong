package com.pingpong.ping.domain;

import java.time.Instant;

/**
 * Ping aggregate root (pure domain — no framework, no OpenTelemetry imports, OQ-2).
 * Creating a Ping produces the {@link PingCreated} domain fact.
 */
public final class Ping {

    public static final String BOUNDED_CONTEXT = "ping";
    public static final String AGGREGATE_TYPE = "Ping";

    private final String pingId;
    private final String note;
    private final Instant createdAt;

    private Ping(String pingId, String note, Instant createdAt) {
        this.pingId = pingId;
        this.note = note;
        this.createdAt = createdAt;
    }

    /** Factory: create a new ping and the fact that describes it. */
    public static PingCreated create(String pingId, String note) {
        Ping ping = new Ping(pingId, note, Instant.now());
        return new PingCreated(ping.pingId, ping.note, ping.createdAt);
    }

    public String ping_id() {
        return pingId;
    }

    public String note() {
        return note;
    }

    public Instant created_at() {
        return createdAt;
    }
}
