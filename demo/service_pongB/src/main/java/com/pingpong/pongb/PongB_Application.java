package com.pingpong.pongb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** service_pongB — a second "pong-b" bounded context subscribing to ping.events (fan-out). */
@SpringBootApplication
public class PongBApplication {

    public static void main(String[] args) {
        SpringApplication.run(PongBApplication.class, args);
    }
}
