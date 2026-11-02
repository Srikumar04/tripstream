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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Phase 2 done-when: a 100k-row file with 5% bad rows and 2% duplicates produces exactly the right good/bad
 * counts, and re-delivering any message changes nothing. Runs api + splitter + worker with the real generator.
 */
@Testcontainers
class PipelineIT {

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
    private static final int ROWS = 100_000;
    private static final int CHUNK_ROWS = 5_000;

    @TempDir
    Path tmp;

    private static StaticCredentialsProvider creds() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
    }

    private Path generate() throws Exception {
        Path csv = tmp.resolve("trips.csv");
        Process p = new ProcessBuilder("python3", "scripts/generate_trips.py", "--rows", String.valueOf(ROWS),
                "--bad-pct", "5", "--dup-pct", "2", "--out", csv.toString()).inheritIO().start();
        assertThat(p.waitFor()).isZero();
        return csv;
    }

    @Test
    void hundredThousandRowsProduceExactCountsAndRedeliveryChangesNothing() throws Exception {
        Path csv = generate();
        JsonNode manifest = JSON.readTree(Files.readString(Path.of(csv + ".manifest.json")));
        int expectedGood = manifest.get("expected_good_rows").asInt();
        int expectedBad = manifest.get("expected_bad_rows").asInt();
        int expectedChunks = ROWS / CHUNK_ROWS;

        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TripStreamApplication.class)
                        .web(WebApplicationType.SERVLET).profiles("api", "splitter", "worker")
                        .run("--server.port=0",
                                "--tripstream.splitter.chunk-rows=" + CHUNK_ROWS,
                                "--tripstream.worker.concurrency=4",
                                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                                "--spring.datasource.username=" + POSTGRES.getUsername(),
                                "--spring.datasource.password=" + POSTGRES.getPassword(),
                                "--spring.cloud.aws.endpoint=" + LOCALSTACK.getEndpoint(),
                                "--spring.cloud.aws.region.static=" + LOCALSTACK.getRegion());
                SqsClient sqs = SqsClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                        .region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(creds()).build();
                SnsClient sns = SnsClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                        .region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(creds()).build();
                DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                        .region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(creds()).build()) {

            String base = "http://localhost:" + ctx.getEnvironment().getProperty("local.server.port");
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);

            // Listen to the BatchCompleted event through a throwaway queue subscribed to the topic.
            String eventsUrl = sqs.createQueue(b -> b.queueName("test-batch-events")).queueUrl();
            String eventsArn = sqs.getQueueAttributes(b -> b.queueUrl(eventsUrl)
                    .attributeNames(QueueAttributeName.QUEUE_ARN)).attributes().get(QueueAttributeName.QUEUE_ARN);
            String topicArn = sns.createTopic(b -> b.name("trip-events")).topicArn();
            sns.subscribe(b -> b.topicArn(topicArn).protocol("sqs").endpoint(eventsArn));

            // Upload.
            JsonNode created = JSON.readTree(HTTP.send(HttpRequest.newBuilder(URI.create(base + "/uploads"))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString()).body());
            UUID jobId = UUID.fromString(created.get("jobId").asText());
            assertThat(HTTP.send(HttpRequest.newBuilder(URI.create(created.get("uploadUrl").asText()))
                    .PUT(HttpRequest.BodyPublishers.ofFile(csv)).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);

            await().atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofSeconds(1))
                    .until(() -> job(base, jobId).get("status").asText().equals("COMPLETED"));

            // Exact counts.
            JsonNode job = job(base, jobId);
            assertThat(job.get("totalRows").asInt()).isEqualTo(ROWS);
            assertThat(job.get("goodRows").asInt()).isEqualTo(expectedGood);
            assertThat(job.get("badRows").asInt()).isEqualTo(expectedBad);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM trips WHERE job_id = ?", Integer.class, jobId))
                    .isEqualTo(expectedGood);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM job_chunks WHERE job_id = ? AND status = 'DONE'",
                    Integer.class, jobId)).isEqualTo(expectedChunks);
            assertThat(chunksDone(ddb, jobId)).isEqualTo(expectedChunks);

            // Error report: one CSV row per bad input row, each with a reason.
            JsonNode files = JSON.readTree(HTTP.send(HttpRequest.newBuilder(
                    URI.create(base + "/jobs/" + jobId + "/errors")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body()).get("files");
            int errorRows = 0;
            for (JsonNode f : files) {
                String body = HTTP.send(HttpRequest.newBuilder(URI.create(f.get("url").asText())).GET().build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                errorRows += body.lines().skip(1).filter(l -> !l.isBlank()).count();
            }
            assertThat(errorRows).isEqualTo(expectedBad);

            // BatchCompleted published exactly once so far.
            assertThat(drain(sqs, eventsUrl, Duration.ofSeconds(5))).hasSize(1);

            // Snapshot, then re-deliver EVERY chunk message plus the original file event.
            Map<String, Object> before = snapshot(jdbc, ddb, jobId);
            String chunkQueue = sqs.getQueueUrl(b -> b.queueName("chunk-jobs")).queueUrl();
            for (int c = 0; c < expectedChunks; c++) {
                String body = JSON.writeValueAsString(Map.of("jobId", jobId, "chunkId", c,
                        "startRow", (long) c * CHUNK_ROWS + 1, "rowCount", CHUNK_ROWS,
                        "key", "chunks/%s/%06d.csv".formatted(jobId, c)));
                sqs.sendMessage(b -> b.queueUrl(chunkQueue).messageBody(body));
            }
            String fileQueue = sqs.getQueueUrl(b -> b.queueName("file-events")).queueUrl();
            sqs.sendMessage(b -> b.queueUrl(fileQueue).messageBody(
                    "{\"Records\":[{\"s3\":{\"object\":{\"key\":\"uploads/%s/input.csv\"}}}]}".formatted(jobId)));

            awaitDrained(sqs, chunkQueue);
            awaitDrained(sqs, fileQueue);

            assertThat(snapshot(jdbc, ddb, jobId)).isEqualTo(before);
            assertThat(drain(sqs, eventsUrl, Duration.ofSeconds(3))).isEmpty(); // no second BatchCompleted
        }
    }

    private static JsonNode job(String base, UUID id) throws Exception {
        return JSON.readTree(HTTP.send(HttpRequest.newBuilder(URI.create(base + "/jobs/" + id)).GET().build(),
                HttpResponse.BodyHandlers.ofString()).body());
    }

    private static long chunksDone(DynamoDbClient ddb, UUID jobId) {
        Map<String, AttributeValue> item = ddb.getItem(b -> b.tableName("tripstream-job-progress")
                .key(Map.of("jobId", AttributeValue.builder().s(jobId.toString()).build()))
                .consistentRead(true)).item();
        return Long.parseLong(item.get("chunksDone").n());
    }

    /** Everything that must not change when messages are redelivered. */
    private static Map<String, Object> snapshot(JdbcTemplate jdbc, DynamoDbClient ddb, UUID jobId) {
        return Map.of(
                "job", jdbc.queryForMap("SELECT status, total_rows, good_rows, bad_rows, completed_at, "
                        + "completion_event_published_at FROM jobs WHERE job_id = ?", jobId).toString(),
                "trips", jdbc.queryForMap("SELECT count(*) AS n, max(updated_at) AS last_write FROM trips "
                        + "WHERE job_id = ?", jobId).toString(),
                "attempts", String.valueOf(jdbc.queryForObject(
                        "SELECT sum(attempts) FROM job_chunks WHERE job_id = ?", Long.class, jobId)),
                "chunksDone", chunksDone(ddb, jobId));
    }

    private static void awaitDrained(SqsClient sqs, String queueUrl) {
        // In-flight messages count as "not visible" until deleted, so zero/zero means fully processed.
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            var a = sqs.getQueueAttributes(b -> b.queueUrl(queueUrl).attributeNames(
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)).attributes();
            assertThat(a.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)).isEqualTo("0");
            assertThat(a.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)).isEqualTo("0");
        });
    }

    private static List<Message> drain(SqsClient sqs, String queueUrl, Duration window) throws Exception {
        List<Message> all = new ArrayList<>();
        long end = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < end) {
            all.addAll(sqs.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(1))
                    .messages());
        }
        return all;
    }
}
