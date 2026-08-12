package com.pingpong.bang.domain;

import java.time.Instant;

/** Internal domain fact: the ping → pong → bang chain finished at this terminal service. */
public record ChainCompleted(String pingId, Instant completedAt) {
}
