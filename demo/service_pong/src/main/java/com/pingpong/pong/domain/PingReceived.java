package com.pingpong.pong.domain;

import java.time.Instant;

/** Internal domain fact: a ping was received by this context (not published across services). */
public record PingReceived(String pingId, Instant receivedAt) {
}
