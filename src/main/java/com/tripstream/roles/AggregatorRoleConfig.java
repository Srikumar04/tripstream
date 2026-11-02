package com.tripstream.roles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Role wiring for the "aggregator" profile. Role-specific beans are added here in later phases. */
@Configuration
@Profile("aggregator")
class AggregatorRoleConfig {
    private static final Logger log = LoggerFactory.getLogger(AggregatorRoleConfig.class);

    @Bean
    RoleMarker roleMarker() {
        log.info("Starting TripStream role: aggregator");
        return new RoleMarker("aggregator");
    }
}
