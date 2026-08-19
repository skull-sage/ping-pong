package com.pingpong.ping.domain;

import java.time.Instant;

/**
 * Domain fact: a ping was created. Plain data — the infrastructure adapter later wraps it in an
 * {@code EventEnvelope} and stamps the tracing attributes.
 *
 * <p>{@code faulty} marks the failure-visualization scenario: the event still travels the normal
 * {@code ping.events} flow, but downstream service_pong logs an ERROR for it instead of responding.
 */
public record PingCreated(String pingId, String note, boolean faulty, Instant createdAt) {
}
