package com.tripstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.tripstream.roles.RoleMarker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Each role starts independently under its own profile and applies the Liquibase schema. */
@Testcontainers
class RoleProfilesIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
                    DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(
                    LocalStackContainer.EnabledService.named("s3"),
                    LocalStackContainer.EnabledService.named("sqs"),
                    LocalStackContainer.EnabledService.named("sns"),
                    LocalStackContainer.EnabledService.named("dynamodb"))
            .withCopyFileToContainer(
                    MountableFile.forHostPath("infra/localstack/init-aws.sh", 0755),
                    "/etc/localstack/init/ready.d/init-aws.sh")
            .waitingFor(Wait.forLogMessage(".*TripStream local infra ready.*", 1));

    private ConfigurableApplicationContext start(String profile) {
        return new SpringApplicationBuilder(TripStreamApplication.class)
                .web(WebApplicationType.SERVLET)
                .profiles(profile)
                .run(
                        "--server.port=0",
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.cloud.aws.endpoint=" + LOCALSTACK.getEndpoint(),
                        "--spring.cloud.aws.region.static=" + LOCALSTACK.getRegion());
    }

    @ParameterizedTest
    @ValueSource(strings = {"api", "splitter", "worker", "aggregator", "notifier"})
    void eachProfileStartsAsExactlyItsOwnRole(String profile) {
        try (ConfigurableApplicationContext ctx = start(profile)) {
            assertThat(ctx.getBeansOfType(RoleMarker.class).values())
                    .extracting(RoleMarker::role)
                    .containsExactly(profile);
        }
    }

    @Test
    void liquibaseCreatesSchema() {
        try (ConfigurableApplicationContext ctx = start("api")) {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            Integer tables = jdbc.queryForObject(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' "
                            + "AND table_name IN ('trips','jobs','job_chunks','daily_vehicle_stats')",
                    Integer.class);
            assertThat(tables).isEqualTo(4);
        }
    }
}
