package com.pingpong.ping;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * service_ping — the "ping" bounded context.
 *
 * <p>Entry point of the simulation: an HTTP call produces a {@code PingCreated} integration event,
 * which fans out to the pong services; their {@code PongResponded} events fan back in here.
 */
@SpringBootApplication
public class PingApplication {

    public static void main(String[] args) {
        SpringApplication.run(PingApplication.class, args);
    }
}
