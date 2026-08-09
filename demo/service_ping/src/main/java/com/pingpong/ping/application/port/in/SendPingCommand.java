package com.pingpong.ping.application.port.in;

/** Plain command carried into the application layer — no framework/OTel types. */
public record SendPingCommand(String note) {
}
