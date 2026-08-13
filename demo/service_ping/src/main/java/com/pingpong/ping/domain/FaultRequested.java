package com.pingpong.ping.domain;

import java.time.Instant;

/**
 * Domain fact (CR-2 failure pipeline): a caller deliberately requested a fault so the downstream
 * error path can be exercised. Plain data — the infrastructure adapter wraps it in an
 * {@code EventEnvelope} and publishes it to {@code ping.faults}, where service_pong raises a
 * logged exception.
 */
public record FaultRequested(String pingId, String reason, Instant requestedAt) {
}
