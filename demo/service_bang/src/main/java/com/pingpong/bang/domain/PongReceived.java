package com.pingpong.bang.domain;

import java.time.Instant;

/** Internal domain fact: a pong (from service_pong) was received by the bang context. */
public record PongReceived(String pingId, String responder, Instant receivedAt) {
}
