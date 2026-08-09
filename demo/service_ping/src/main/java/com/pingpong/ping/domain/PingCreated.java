package com.pingpong.ping.domain;

import java.time.Instant;

/**
 * Domain fact: a ping was created. Plain data — the infrastructure adapter later wraps it in an
 * {@code EventEnvelope} and stamps the tracing attributes.
 */
public record PingCreated(String pingId, String note, Instant createdAt) {
}
