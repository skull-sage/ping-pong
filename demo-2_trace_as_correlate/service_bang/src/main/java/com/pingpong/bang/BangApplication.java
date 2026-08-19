package com.pingpong.bang;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** service_bang — a second "bang" bounded context subscribing to ping.events (fan-out). */
@SpringBootApplication
public class BangApplication {

    public static void main(String[] args) {
        SpringApplication.run(BangApplication.class, args);
    }
}
