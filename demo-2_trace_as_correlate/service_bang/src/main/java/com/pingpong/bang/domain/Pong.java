package com.pingpong.bang.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Terminal aggregate for the bang context — the last link of the ping → pong → bang chain.
 * It consumes the pong fact and finalizes the flow. Two mutating actions, each raising an
 * internal domain event (bang publishes nothing downstream):
 * <ol>
 *   <li>{@link #receive(String, String)} -> {@link PongReceived}</li>
 *   <li>{@link #complete()}              -> {@link ChainCompleted}</li>
 * </ol>
 */
public final class Pong {

    public static final String BOUNDED_CONTEXT = "bang";
    public static final String AGGREGATE_TYPE = "Bang";
    public static final String CONSUMER = "service-bang";

    private final String pingId;
    private String responder;
    private String status;
    private final List<Object> pending_events = new ArrayList<>();

    private Pong(String pingId) {
        this.pingId = pingId;
    }

    /** Action #1 (mutation): record the incoming pong and raise {@link PongReceived}. */
    public static Pong receive(String pingId, String responder) {
        Pong pong = new Pong(pingId);
        pong.responder = responder;
        pong.status = "RECEIVED";
        pong.pending_events.add(new PongReceived(pingId, responder, Instant.now()));
        return pong;
    }

    /** Action #2 (mutation): finalize the chain and raise {@link ChainCompleted}. */
    public void complete() {
        this.status = "COMPLETED";
        this.pending_events.add(new ChainCompleted(pingId, Instant.now()));
    }

    public List<Object> pull_events() {
        List<Object> drained = List.copyOf(pending_events);
        pending_events.clear();
        return drained;
    }

    public String ping_id() {
        return pingId;
    }

    public String responder() {
        return responder;
    }

    public String status() {
        return status;
    }
}
