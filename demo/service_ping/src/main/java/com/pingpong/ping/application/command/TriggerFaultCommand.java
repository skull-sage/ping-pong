package com.pingpong.ping.application.command;

/**
 * Command emitted by the REST controller to start the CR-2 failure pipeline: it publishes a
 * fault-request event that service_pong consumes and deliberately fails on.
 */
public record TriggerFaultCommand(String reason) {
}
