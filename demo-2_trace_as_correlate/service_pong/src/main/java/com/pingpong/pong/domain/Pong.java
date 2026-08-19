package com.pingpong.pong.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Pong aggregate root (pure domain) for the pong bounded context. Two mutating actions, each
 * raising a domain event:
 * <ol>
 *   <li>{@link #receive(String)} -> {@link PingReceived} (internal domain event)</li>
 *   <li>{@link #respond()}       -> {@link PongResponded} (integration event, published)</li>
 * </ol>
 */
public final class Pong {

    public static final String BOUNDED_CONTEXT = "pong";
    public static final String AGGREGATE_TYPE = "Pong";
    public static final String RESPONDER = "service-pong";

    private final String pingId;
    private String status;
    private final List<Object> pending_events = new ArrayList<>();

    private Pong(String pingId) {
        this.pingId = pingId;
    }

    /** Action #1 (mutation): register an incoming ping and raise {@link PingReceived}. */
    public static Pong receive(String pingId) {
        Pong pong = new Pong(pingId);
        pong.status = "RECEIVED";
        pong.pending_events.add(new PingReceived(pingId, Instant.now()));
        return pong;
    }

    /** Action #2 (mutation): respond to the ping and raise {@link PongResponded}. */
    public void respond() {
        this.status = "RESPONDED";
        this.pending_events.add(new PongResponded(pingId, RESPONDER, Instant.now()));
    }

    public List<Object> pull_events() {
        List<Object> drained = List.copyOf(pending_events);
        pending_events.clear();
        return drained;
    }

    public String ping_id() {
        return pingId;
    }

    public String status() {
        return status;
    }
}
