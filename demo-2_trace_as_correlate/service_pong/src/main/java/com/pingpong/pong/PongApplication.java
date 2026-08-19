package com.pingpong.pong;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** service_pong — a "pong" bounded context subscribing to ping.events (event fan-out). */
@SpringBootApplication
public class PongApplication {

    public static void main(String[] args) {
        SpringApplication.run(PongApplication.class, args);
    }
}
