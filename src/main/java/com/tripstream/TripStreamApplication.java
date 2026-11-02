package com.tripstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Single codebase, five independently runnable roles selected by Spring profile:
 * api, splitter, worker, aggregator, notifier.
 * Run one with: --spring.profiles.active=worker
 */
@SpringBootApplication
public class TripStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(TripStreamApplication.class, args);
    }
}
