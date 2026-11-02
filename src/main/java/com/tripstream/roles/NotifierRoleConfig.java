package com.tripstream.roles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Role wiring for the "notifier" profile. Role-specific beans are added here in later phases. */
@Configuration
@Profile("notifier")
class NotifierRoleConfig {
    private static final Logger log = LoggerFactory.getLogger(NotifierRoleConfig.class);

    @Bean("notifierRoleMarker")
    RoleMarker notifierRoleMarker() {
        log.info("Starting TripStream role: notifier");
        return new RoleMarker("notifier");
    }
}
