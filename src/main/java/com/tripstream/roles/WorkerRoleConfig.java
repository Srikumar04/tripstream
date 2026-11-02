package com.tripstream.roles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Role wiring for the "worker" profile. Role-specific beans are added here in later phases. */
@Configuration
@Profile("worker")
class WorkerRoleConfig {
    private static final Logger log = LoggerFactory.getLogger(WorkerRoleConfig.class);

    @Bean("workerRoleMarker")
    RoleMarker workerRoleMarker() {
        log.info("Starting TripStream role: worker");
        return new RoleMarker("worker");
    }
}
