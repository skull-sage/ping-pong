package com.pingpong.ping.domain;

import java.time.Instant;

/** Domain fact: a returning pong was acknowledged, closing the ping's side of the saga. */
public record PongAcknowledged(String pingId, String responder, Instant acknowledgedAt) {
}
