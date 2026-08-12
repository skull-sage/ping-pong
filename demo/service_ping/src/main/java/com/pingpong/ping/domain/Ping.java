package com.pingpong.ping.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Ping aggregate root (pure domain — no Spring/OpenTelemetry, per clean architecture).
 *
 * <p>Exposes two state-mutating actions, each of which raises a domain event:
 * <ol>
 *   <li>{@link #create(String, String)} -> {@link PingCreated}</li>
 *   <li>{@link #acknowledge(String)}     -> {@link PongAcknowledged}</li>
 * </ol>
 * Raised events are buffered and drained by the application layer via {@link #pull_events()}.
 */
public final class Ping {

    public static final String BOUNDED_CONTEXT = "ping";
    public static final String AGGREGATE_TYPE = "Ping";

    private final String id;
    private String note;
    private String status;
    private final List<Object> pending_events = new ArrayList<>();

    private Ping(String id) {
        this.id = id;
    }

    /** Action #1 (mutation): create a new ping and raise {@link PingCreated}. */
    public static Ping create(String id, String note) {
        Ping ping = new Ping(id);
        ping.note = note;
        ping.status = "CREATED";
        ping.pending_events.add(new PingCreated(id, note, Instant.now()));
        return ping;
    }

    /** Rehydrate a known ping without raising an event (for the acknowledge flow). */
    public static Ping rehydrate(String id) {
        Ping ping = new Ping(id);
        ping.status = "CREATED";
        return ping;
    }

    /** Action #2 (mutation): acknowledge a returning pong and raise {@link PongAcknowledged}. */
    public void acknowledge(String responder) {
        this.status = "ACKNOWLEDGED";
        this.pending_events.add(new PongAcknowledged(id, responder, Instant.now()));
    }

    /** Drains the buffered domain events (the application layer publishes/handles them). */
    public List<Object> pull_events() {
        List<Object> drained = List.copyOf(pending_events);
        pending_events.clear();
        return drained;
    }

    public String id() {
        return id;
    }

    public String note() {
        return note;
    }

    public String status() {
        return status;
    }
}
