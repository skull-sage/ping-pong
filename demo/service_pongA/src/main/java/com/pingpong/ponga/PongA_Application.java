package com.pingpong.ponga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** service_pongA — a "pong-a" bounded context subscribing to ping.events (event fan-out). */
@SpringBootApplication
public class PongA_Application {

    public static void main(String[] args) {
        SpringApplication.run(PongA_Application.class, args);
    }
}
