package com.tripstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/** Shared containers (started once per JVM) and helpers for the failure-mode tests. */
final class TestEnv {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
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

    static {
        POSTGRES.start();
        LOCALSTACK.start();
    }

    static final ObjectMapper JSON = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final JdbcTemplate JDBC = new JdbcTemplate(new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));

    private TestEnv() {
    }

    record Dataset(Path csv, int rows, int expectedGood, int expectedBad) {
    }

    // ---- clients -----------------------------------------------------------------------------------------------

    private static StaticCredentialsProvider creds() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
    }

    static SqsClient sqs() {
        return SqsClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(creds()).build();
    }

    static DynamoDbClient ddb() {
        return DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                .region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(creds()).build();
    }

    static S3Client s3() {
        return S3Client.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(creds()).forcePathStyle(true).build();
    }

    // ---- app contexts ------------------------------------------------------------------------------------------

    /** Spring/AWS settings every role needs to talk to the test containers. */
    static List<String> connectionArgs() {
        return List.of("--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.cloud.aws.endpoint=" + LOCALSTACK.getEndpoint(),
                "--spring.cloud.aws.region.static=" + LOCALSTACK.getRegion());
    }

    static ConfigurableApplicationContext start(List<String> profiles, String... extraArgs) {
        List<String> args = new ArrayList<>(connectionArgs());
        args.addAll(List.of(extraArgs));
        return new SpringApplicationBuilder(TripStreamApplication.class).web(WebApplicationType.SERVLET)
                .profiles(profiles.toArray(String[]::new)).run(args.toArray(String[]::new));
    }

    static String base(ConfigurableApplicationContext ctx) {
        return "http://localhost:" + ctx.getEnvironment().getProperty("local.server.port");
    }

    // ---- queues ------------------------------------------------------------------------------------------------

    /** Clean slate: empty the pipeline queues and DLQs and set the visibility timeout used by the test. */
    static void resetQueues(int visibilitySeconds) {
        try (SqsClient sqs = sqs()) {
            for (String q : List.of("file-events", "chunk-jobs", "file-events-dlq", "chunk-jobs-dlq")) {
                sqs.purgeQueue(b -> b.queueUrl(url(sqs, q)));
            }
            for (String q : List.of("file-events", "chunk-jobs")) {
                sqs.setQueueAttributes(b -> b.queueUrl(url(sqs, q)).attributes(
                        Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilitySeconds))));
            }
        }
    }

    static void purge(String... queues) {
        try (SqsClient sqs = sqs()) {
            for (String q : queues) {
                sqs.purgeQueue(b -> b.queueUrl(url(sqs, q)));
            }
        }
    }

    /** Collects every message that arrives within the window (used to assert "exactly N" without sleeping blind). */
    static List<software.amazon.awssdk.services.sqs.model.Message> drain(SqsClient sqs, String queueUrl,
            Duration window) {
        List<software.amazon.awssdk.services.sqs.model.Message> all = new ArrayList<>();
        long end = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < end) {
            all.addAll(sqs.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(1))
                    .messages());
        }
        return all;
    }

    static String url(SqsClient sqs, String queue) {
        return sqs.getQueueUrl(b -> b.queueName(queue)).queueUrl();
    }

    static int depth(SqsClient sqs, String queue) {
        var a = sqs.getQueueAttributes(b -> b.queueUrl(url(sqs, queue)).attributeNames(
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED)).attributes();
        return a.values().stream().mapToInt(Integer::parseInt).sum();
    }

    // ---- data and jobs -----------------------------------------------------------------------------------------

    static Dataset generate(Path dir, int rows) throws Exception {
        Path csv = dir.resolve("trips-" + rows + "-" + UUID.randomUUID() + ".csv");
        Process p = new ProcessBuilder("python3", "scripts/generate_trips.py", "--rows", String.valueOf(rows),
                "--bad-pct", "5", "--dup-pct", "2", "--out", csv.toString()).inheritIO().start();
        assertThat(p.waitFor()).isZero();
        JsonNode m = JSON.readTree(Files.readString(Path.of(csv + ".manifest.json")));
        return new Dataset(csv, rows, m.get("expected_good_rows").asInt(), m.get("expected_bad_rows").asInt());
    }

    static UUID upload(String base, Path csv) throws Exception {
        JsonNode created = JSON.readTree(HTTP.send(HttpRequest.newBuilder(URI.create(base + "/uploads"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString()).body());
        assertThat(HTTP.send(HttpRequest.newBuilder(URI.create(created.get("uploadUrl").asText()))
                .PUT(HttpRequest.BodyPublishers.ofFile(csv)).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        return UUID.fromString(created.get("jobId").asText());
    }

    static String status(UUID jobId) {
        return JDBC.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, jobId);
    }

    static void awaitStatus(UUID jobId, String status, Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(500)).ignoreExceptions()
                .until(() -> status(jobId).equals(status));
    }

    static void assertExact(UUID jobId, Dataset d, int expectedChunks) {
        var job = JDBC.queryForMap("SELECT status, total_rows, good_rows, bad_rows FROM jobs WHERE job_id = ?", jobId);
        assertThat(job.get("status")).isEqualTo("COMPLETED");
        assertThat(((Number) job.get("total_rows")).intValue()).isEqualTo(d.rows());
        assertThat(((Number) job.get("good_rows")).intValue()).isEqualTo(d.expectedGood());
        assertThat(((Number) job.get("bad_rows")).intValue()).isEqualTo(d.expectedBad());
        assertThat(JDBC.queryForObject("SELECT count(*) FROM trips WHERE job_id = ?", Integer.class, jobId))
                .isEqualTo(d.expectedGood());
        assertThat(JDBC.queryForObject("SELECT count(*) FROM job_chunks WHERE job_id = ? AND status = 'DONE'",
                Integer.class, jobId)).isEqualTo(expectedChunks);
        assertThat(chunksDone(jobId)).as("DynamoDB chunksDone (must be counted exactly once per chunk)")
                .isEqualTo(expectedChunks);
    }

    static long chunksDone(UUID jobId) {
        try (DynamoDbClient ddb = ddb()) {
            var item = ddb.getItem(b -> b.tableName("tripstream-job-progress")
                    .key(Map.of("jobId", AttributeValue.builder().s(jobId.toString()).build()))
                    .consistentRead(true)).item();
            return Long.parseLong(item.get("chunksDone").n());
        }
    }

    static int maxAttempts(UUID jobId) {
        return JDBC.queryForObject("SELECT max(attempts) FROM job_chunks WHERE job_id = ?", Integer.class, jobId);
    }

    static int claimsInProgress(UUID jobId) {
        try (DynamoDbClient ddb = ddb()) {
            return ddb.scan(b -> b.tableName("tripstream-idempotency")
                    .filterExpression("begins_with(pk, :p) AND #s = :ip")
                    .expressionAttributeNames(Map.of("#s", "status"))
                    .expressionAttributeValues(Map.of(
                            ":p", AttributeValue.builder().s(jobId + "#").build(),
                            ":ip", AttributeValue.builder().s("IN_PROGRESS").build()))).count();
        }
    }

    static String chunkMessage(UUID jobId, int chunkId, int chunkRows) throws Exception {
        return JSON.writeValueAsString(Map.of("jobId", jobId, "chunkId", chunkId,
                "startRow", (long) chunkId * chunkRows + 1, "rowCount", chunkRows,
                "key", "chunks/%s/%06d.csv".formatted(jobId, chunkId)));
    }
}
