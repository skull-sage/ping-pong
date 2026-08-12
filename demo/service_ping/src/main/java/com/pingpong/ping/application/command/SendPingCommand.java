package com.pingpong.ping.application.command;

/** Command emitted by the REST controller to start one ping-pong saga. */
public record SendPingCommand(String note) {
}
