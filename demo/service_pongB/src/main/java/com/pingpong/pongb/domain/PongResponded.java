package com.pingpong.pongb.domain;

import java.time.Instant;

/** Domain fact: this service responded to a ping. */
public record PongResponded(String pingId, String responder, Instant respondedAt) {
}
