package com.tripstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

/** Phase 1 done-when: uploading a file moves the job to RECEIVED and a message appears on file-events. */
@Testcontainers
class IngestionIT {

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

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final String CSV = "trip_id,vehicle_id,driver_id,start_time,end_time,distance_km,status\n"
            + "T1,V1,D1,2025-01-01T10:00:00Z,2025-01-01T11:00:00Z,12.50,COMPLETED\n";

    private record Upload(UUID jobId, String uploadUrl) {
    }

    private ConfigurableApplicationContext start(String... profiles) {
        return new SpringApplicationBuilder(TripStreamApplication.class)
                .web(WebApplicationType.SERVLET)
                .profiles(profiles)
                .run("--server.port=0",
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.cloud.aws.endpoint=" + LOCALSTACK.getEndpoint(),
                        "--spring.cloud.aws.region.static=" + LOCALSTACK.getRegion());
    }

    private static String base(ConfigurableApplicationContext ctx) {
        return "http://localhost:" + ctx.getEnvironment().getProperty("local.server.port");
    }

    private static Upload createUpload(String base) throws Exception {
        HttpResponse<String> r = HTTP.send(
                HttpRequest.newBuilder(URI.create(base + "/uploads")).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(201);
        JsonNode n = JSON.readTree(r.body());
        return new Upload(UUID.fromString(n.get("jobId").asText()), n.get("uploadUrl").asText());
    }

    private static void putFile(Upload u) throws Exception {
        HttpResponse<String> r = HTTP.send(
                HttpRequest.newBuilder(URI.create(u.uploadUrl())).PUT(HttpRequest.BodyPublishers.ofString(CSV)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(200);
    }

    private static HttpResponse<String> getJob(String base, String id) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(base + "/jobs/" + id)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String status(String base, UUID id) throws Exception {
        return JSON.readTree(getJob(base, id.toString()).body()).get("status").asText();
    }

    private static SqsClient sqs() {
        return SqsClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();
    }

    @Test
    void uploadPutsAnObjectCreatedMessageOnFileEventsAndJobStaysCreatedWithoutAConsumer() throws Exception {
        try (ConfigurableApplicationContext ctx = start("api"); SqsClient sqs = sqs()) {
            String base = base(ctx);
            Upload u = createUpload(base);
            assertThat(status(base, u.jobId())).isEqualTo("CREATED");

            putFile(u);

            String queueUrl = sqs.getQueueUrl(b -> b.queueName("file-events")).queueUrl();
            List<String> bodies = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                for (Message m : sqs.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10)
                        .waitTimeSeconds(1)).messages()) {
                    bodies.add(m.body());
                }
                assertThat(bodies).anyMatch(b -> b.contains("uploads/" + u.jobId() + "/input.csv")
                        && b.contains("ObjectCreated"));
            });
            assertThat(status(base, u.jobId())).isEqualTo("CREATED");
        }
    }

    @Test
    void splitterRoleAdvancesJobAndDuplicateOrUnknownEventsAreHarmless() throws Exception {
        try (ConfigurableApplicationContext ctx = start("api", "splitter"); SqsClient sqs = sqs()) {
            String base = base(ctx);
            Upload u = createUpload(base);
            putFile(u);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(
                    () -> assertThat(status(base, u.jobId())).isEqualTo("PROCESSING"));

            // Redeliver the same event and send one for a job that does not exist.
            String queueUrl = sqs.getQueueUrl(b -> b.queueName("file-events")).queueUrl();
            String event = "{\"Records\":[{\"s3\":{\"object\":{\"key\":\"uploads/%s/input.csv\"}}}]}";
            sqs.sendMessage(b -> b.queueUrl(queueUrl).messageBody(event.formatted(u.jobId())));
            sqs.sendMessage(b -> b.queueUrl(queueUrl).messageBody(event.formatted(UUID.randomUUID())));

            // A later job still completes, proving the listener is not stuck, and the first job is unchanged.
            Upload next = createUpload(base);
            putFile(next);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(
                    () -> assertThat(status(base, next.jobId())).isEqualTo("PROCESSING"));
            assertThat(status(base, u.jobId())).isEqualTo("PROCESSING");
        }
    }

    @Test
    void jobEndpointReturns404ForUnknownAnd400ForMalformedIds() throws Exception {
        try (ConfigurableApplicationContext ctx = start("api")) {
            String base = base(ctx);
            assertThat(getJob(base, UUID.randomUUID().toString()).statusCode()).isEqualTo(404);
            assertThat(getJob(base, "not-a-uuid").statusCode()).isEqualTo(400);
        }
    }
}
