package com.pingpong.common;

/**
 * Kafka topics that carry integration events across service boundaries.
 * Only integration events cross the wire (DD-2).
 */
public final class Topics {

    /** Fact published by service_ping; fanned out to every pong subscriber. */
    public static final String PING_EVENTS = "ping.events";

    /** Fact published by each pong service; consumed back by service_ping (fan-in). */
    public static final String PONG_EVENTS = "pong.events";

    private Topics() {
    }
}
