package com.tripstream;

import static com.tripstream.TestEnv.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Phase 3: every failure mode in docs/failure-modes.md, injected for real, ending in the same assertion:
 * exact good/bad counts, every chunk counted exactly once. No Thread.sleep for synchronisation (Awaitility only;
 * the few fixed waits are deliberate "let the failure window elapse" periods and are called out).
 */
class FailureModesIT {

    private static final int CHUNK_ROWS = 2_000;

    /** Short timings so a lease/visibility expiry takes seconds, not minutes. */
    private static String[] fast(String... extra) {
        List<String> a = new ArrayList<>(List.of(
                "--tripstream.splitter.chunk-rows=" + CHUNK_ROWS,
                "--tripstream.worker.concurrency=2", "--tripstream.worker.poll-size=2",
                "--tripstream.reliability.retry-base-delay=PT1S", "--tripstream.reliability.retry-max-delay=PT2S"));
        a.addAll(List.of(extra));
        return a.toArray(String[]::new);
    }

    @TempDir
    Path tmp;

    // ------------------------------------------------------------------------------------------------------------
    @Test
    void workerKilledMidChunkIsRecoveredByAnotherWorker() throws Exception {
        Path jar = Path.of("target/tripstream-0.1.0-SNAPSHOT.jar");
        assumeThat(Files.exists(jar)).as("packaged jar (built by mvn verify)").isTrue();
        resetQueues(8);
        Dataset d = generate(tmp, 20_000);
        String[] timings = {"--tripstream.worker.lease=PT4S", "--tripstream.reliability.heartbeat-interval=PT1S",
                "--tripstream.reliability.visibility-timeout=PT8S"};

        UUID jobId;
        try (ConfigurableApplicationContext front = start(List.of("api", "splitter"), fast(timings))) {
            // A real worker process that stalls inside every chunk, so we can kill -9 it mid-flight.
            List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-jar", jar.toString(), "--spring.profiles.active=worker"));
            cmd.addAll(connectionArgs());
            cmd.addAll(List.of(fast(timings)));
            cmd.add("--tripstream.chaos.chunk-delay=PT120S");
            Process victim = new ProcessBuilder(cmd).redirectErrorStream(true)
                    .redirectOutput(tmp.resolve("victim.log").toFile()).start();
            try {
                await().atMost(Duration.ofSeconds(90)).pollDelay(Duration.ofSeconds(2)).until(() -> {
                    // wait for the victim to be up (its listener is polling) before uploading
                    return new File(tmp.resolve("victim.log").toString()).length() > 0
                            && Files.readString(tmp.resolve("victim.log")).contains("Started TripStreamApplication");
                });
                jobId = upload(base(front), d.csv());
                // Claimed in DynamoDB AND recorded as started in Postgres, i.e. genuinely mid-chunk.
                await().atMost(Duration.ofSeconds(60)).until(() -> claimsInProgress(jobId) >= 1 && JDBC.queryForObject(
                        "SELECT count(*) FROM job_chunks WHERE job_id = ? AND attempts >= 1", Integer.class, jobId) >= 1);
            } finally {
                victim.destroyForcibly().waitFor(); // SIGKILL: no shutdown hooks, no ack, no claim release
            }
            assertThat(claimsInProgress(jobId)).as("killed worker left its claims behind").isGreaterThanOrEqualTo(1);
            assertThat(status(jobId)).isEqualTo("PROCESSING");

            try (ConfigurableApplicationContext survivor = start(List.of("worker"), fast(timings))) {
                awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(2));
            }
        }
        assertExact(jobId, d, 10);
        assertThat(maxAttempts(jobId)).as("killed chunks were retried").isGreaterThanOrEqualTo(2);
    }

    // ------------------------------------------------------------------------------------------------------------
    @Test
    void heartbeatKeepsASlowChunkFromBeingReprocessed() throws Exception {
        resetQueues(5);
        Dataset d = generate(tmp, 10_000);
        String[] timings = {"--tripstream.worker.lease=PT3S", "--tripstream.reliability.heartbeat-interval=PT1S",
                "--tripstream.reliability.visibility-timeout=PT5S"};
        try (ConfigurableApplicationContext front = start(List.of("api", "splitter"), fast(timings));
                ConfigurableApplicationContext slow = start(List.of("worker"),
                        fast(append(timings, "--tripstream.chaos.chunk-delay=PT12S")));
                ConfigurableApplicationContext fastWorker = start(List.of("worker"), fast(timings))) {
            UUID jobId = upload(base(front), d.csv());
            awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(2));
            assertExact(jobId, d, 5);
            // Each chunk is 12s of "work" against a 5s visibility timeout / 3s lease. Without a heartbeat the
            // other worker would take them over (see the control test); with it, nothing is reprocessed.
            assertThat(maxAttempts(jobId)).isEqualTo(1);
        }
    }

    @Test
    void controlWithoutHeartbeatSlowChunkIsReprocessedYetCountsStayExact() throws Exception {
        resetQueues(5);
        Dataset d = generate(tmp, 10_000);
        String[] timings = {"--tripstream.worker.lease=PT3S", "--tripstream.reliability.heartbeat-interval=PT0S",
                "--tripstream.reliability.visibility-timeout=PT5S"};
        try (ConfigurableApplicationContext front = start(List.of("api", "splitter"), fast(timings));
                ConfigurableApplicationContext slow = start(List.of("worker"),
                        fast(append(timings, "--tripstream.chaos.chunk-delay=PT12S")));
                ConfigurableApplicationContext fastWorker = start(List.of("worker"), fast(timings))) {
            UUID jobId = upload(base(front), d.csv());
            awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(2));
            // The slow worker wakes up after the job finished and redoes a chunk another worker already finished.
            await().atMost(Duration.ofMinutes(1)).until(() -> maxAttempts(jobId) >= 2);
            // ...and once that duplicate work completes, nothing has been counted twice.
            await().atMost(Duration.ofMinutes(1)).until(() -> JDBC.queryForObject(
                    "SELECT count(*) FROM job_chunks WHERE job_id = ? AND finished_at IS NOT NULL", Integer.class,
                    jobId) == 5);
            assertExact(jobId, d, 5);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    @Test
    void duplicateMessagesDeliveredWhileTheChunkIsInFlightAreNotDoubleCounted() throws Exception {
        resetQueues(8);
        Dataset d = generate(tmp, 10_000);
        String[] timings = {"--tripstream.worker.lease=PT8S", "--tripstream.reliability.heartbeat-interval=PT2S",
                "--tripstream.reliability.visibility-timeout=PT8S", "--tripstream.chaos.chunk-delay=PT3S"};
        try (ConfigurableApplicationContext all = start(List.of("api", "splitter", "worker"), fast(timings));
                ConfigurableApplicationContext second = start(List.of("worker"), fast(timings));
                SqsClient sqs = sqs()) {
            UUID jobId = upload(base(all), d.csv());
            awaitStatus(jobId, "PROCESSING", Duration.ofSeconds(60));
            String queue = url(sqs, "chunk-jobs");
            for (int c = 0; c < 5; c++) { // second copy of every chunk, while the first copies are mid-flight
                String body = chunkMessage(jobId, c, CHUNK_ROWS);
                sqs.sendMessage(b -> b.queueUrl(queue).messageBody(body));
            }
            awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(2));
            await().atMost(Duration.ofMinutes(1)).until(() -> depth(sqs, "chunk-jobs") == 0);
            assertExact(jobId, d, 5);
            assertThat(maxAttempts(jobId)).as("duplicates were skipped, not reprocessed").isEqualTo(1);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    @Test
    void poisonMessageGoesToDlqWithBackoffAndDoesNotBlockGoodWork() throws Exception {
        resetQueues(120); // default: without our backoff handler a retry would wait 2 minutes
        Dataset d = generate(tmp, 4_000);
        try (ConfigurableApplicationContext all = start(List.of("api", "splitter", "worker"), fast());
                SqsClient sqs = sqs()) {
            long t0 = System.nanoTime();
            sqs.sendMessage(b -> b.queueUrl(url(sqs, "chunk-jobs")).messageBody("this is not json {{{"));
            UUID jobId = upload(base(all), d.csv());

            awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(1)); // good work flows past the poison message
            assertExact(jobId, d, 2);

            await().atMost(Duration.ofSeconds(60)).until(() -> depth(sqs, "chunk-jobs-dlq") >= 1);
            long seconds = Duration.ofNanos(System.nanoTime() - t0).toSeconds();
            // 5 receives with 1s/2s/2s/2s backoff = ~7s, versus 8+ minutes at the queue's 120s default.
            assertThat(seconds).isLessThan(45);
            var dead = sqs.receiveMessage(b -> b.queueUrl(url(sqs, "chunk-jobs-dlq")).visibilityTimeout(0)
                    .waitTimeSeconds(2)).messages();
            assertThat(dead).extracting(m -> m.body()).contains("this is not json {{{");
            assertThat(depth(sqs, "chunk-jobs")).as("poison message left the main queue").isZero();
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    @Test
    void missingS3ObjectGoesToDlqAndRedriveCompletesTheJobAfterTheFix() throws Exception {
        resetQueues(10);
        Dataset d = generate(tmp, 8_000);
        try (ConfigurableApplicationContext front = start(List.of("api", "splitter"), fast());
                SqsClient sqs = sqs(); var s3 = s3()) {
            UUID jobId = upload(base(front), d.csv());
            awaitStatus(jobId, "PROCESSING", Duration.ofSeconds(60)); // chunks written and enqueued, no worker yet

            String key = "chunks/%s/%06d.csv".formatted(jobId, 2);
            byte[] original = s3.getObjectAsBytes(b -> b.bucket("tripstream-chunks").key(key)).asByteArray();
            s3.deleteObject(b -> b.bucket("tripstream-chunks").key(key));

            try (ConfigurableApplicationContext worker = start(List.of("worker"), fast())) {
                await().atMost(Duration.ofSeconds(90)).until(() -> depth(sqs, "chunk-jobs-dlq") >= 1);
                assertThat(status(jobId)).isEqualTo("PROCESSING");
                assertThat(chunksDone(jobId)).isEqualTo(3);
                assertThat(JDBC.queryForObject("SELECT status FROM job_chunks WHERE job_id = ? AND chunk_id = 2",
                        String.class, jobId)).isNotEqualTo("DONE");

                // Operator fixes the cause (restores the object) and redrives the DLQ.
                s3.putObject(b -> b.bucket("tripstream-chunks").key(key), RequestBody.fromBytes(original));
                HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(
                        URI.create(base(front) + "/admin/redrive/chunk-jobs")).POST(HttpRequest.BodyPublishers.noBody())
                        .build(), HttpResponse.BodyHandlers.ofString());
                assertThat(JSON.readTree(r.body()).get("moved").asInt()).isEqualTo(1);

                awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(1));
            }
            assertExact(jobId, d, 4);
            assertThat(depth(sqs, "chunk-jobs-dlq")).isZero();
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    @Test
    void databaseOutageIsRetriedWithBackoffAndLosesNothing() throws Exception {
        resetQueues(10);
        Dataset d = generate(tmp, 20_000);
        String[] timings = {"--tripstream.worker.lease=PT10S", "--tripstream.reliability.heartbeat-interval=PT2S",
                "--tripstream.reliability.visibility-timeout=PT10S", "--tripstream.chaos.chunk-delay=PT1S",
                "--spring.datasource.hikari.connection-timeout=2000",
                "--spring.datasource.hikari.data-source-properties.socketTimeout=3"};
        UUID jobId;
        try (ConfigurableApplicationContext all = start(List.of("api", "splitter", "worker"), fast(timings))) {
            jobId = upload(base(all), d.csv());
            await().atMost(Duration.ofSeconds(60)).ignoreExceptions().until(() -> JDBC.queryForObject(
                    "SELECT count(*) FROM job_chunks WHERE job_id = ? AND status = 'DONE'", Integer.class, jobId) >= 1);

            var docker = POSTGRES.getDockerClient();
            docker.pauseContainerCmd(POSTGRES.getContainerId()).exec(); // DB stops answering mid-run
            try {
                // Deliberate fixed outage window: chunks in flight must hit the dead DB and fail.
                Thread.sleep(Duration.ofSeconds(9).toMillis());
            } finally {
                docker.unpauseContainerCmd(POSTGRES.getContainerId()).exec();
            }
            awaitStatus(jobId, "COMPLETED", Duration.ofMinutes(2));
        }
        assertExact(jobId, d, 10);
        assertThat(maxAttempts(jobId)).as("chunks hit by the outage were retried").isGreaterThanOrEqualTo(2);
    }

    private static String[] append(String[] a, String... more) {
        List<String> l = new ArrayList<>(List.of(a));
        l.addAll(List.of(more));
        return l.toArray(String[]::new);
    }
}
