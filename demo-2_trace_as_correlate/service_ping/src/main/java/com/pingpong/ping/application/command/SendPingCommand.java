package com.pingpong.ping.application.command;

/**
 * Command emitted by the REST controller to start one ping-pong saga.
 *
 * @param note   free-text note (or the fault reason when {@code faulty} is true)
 * @param faulty when true, the very same ping.events flow is used, but the downstream pong service
 *               will log an ERROR for it (the failure-visualization scenario). When false, the flow
 *               completes normally all the way to service_bang.
 */
public record SendPingCommand(String note, boolean faulty) {
}
