package com.tripstream;

import static com.tripstream.TestEnv.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.tripstream.chunk.RowValidator;
import com.tripstream.chunk.TripRow;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Phase 4 done-when: completing a job updates stats exactly once even if the event is delivered twice.
 * Full chain: api + splitter + worker + aggregator + notifier, SNS fan-out, real SNS notification adapter.
 */
class FanOutIT {

    private record Stat(int count, BigDecimal km) {
        Stat {
            km = km.setScale(2, java.math.RoundingMode.HALF_UP);
        }
    }

    @TempDir
    Path tmp;

    @Test
    void statsAndNotificationsAreAppliedExactlyOnceDespiteRedeliveryAndOverlappingUploads() throws Exception {
        resetQueues(30);
        purge("aggregator-queue", "aggregator-queue-dlq", "notifier-queue", "notifier-queue-dlq");

        Dataset d = generate(tmp, 20_000);
        Map<String, Stat> expected = oracle(d.csv());
        assertThat(expected).isNotEmpty();

        try (ConfigurableApplicationContext ctx = start(
                        List.of("api", "splitter", "worker", "aggregator", "notifier"),
                        "--tripstream.splitter.chunk-rows=5000", "--tripstream.notifier.adapter=sns");
                SqsClient sqs = sqs(); SnsClient sns = sns()) {

            // The api role has just applied the migrations; start from empty tables so the oracle is exact.
            JDBC.execute("TRUNCATE trips, job_chunks, jobs, daily_vehicle_stats, processed_events");

            // Capture what the SNS notification adapter publishes.
            String notifUrl = sqs.createQueue(b -> b.queueName("test-notifications")).queueUrl();
            sqs.purgeQueue(b -> b.queueUrl(notifUrl));
            String notifArn = sqs.getQueueAttributes(b -> b.queueUrl(notifUrl)
                    .attributeNames(QueueAttributeName.QUEUE_ARN)).attributes().get(QueueAttributeName.QUEUE_ARN);
            String notifTopic = sns.createTopic(b -> b.name("tripstream-notifications")).topicArn();
            sns.subscribe(b -> b.topicArn(notifTopic).protocol("sqs").endpoint(notifArn));

            // ---- job 1 ----------------------------------------------------------------------------------------
            UUID job1 = upload(base(ctx), d.csv());
            awaitStatus(job1, "COMPLETED", Duration.ofMinutes(3));
            await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> assertThat(stats()).isEqualTo(expected));
            assertThat(JDBC.queryForObject("SELECT count(*) FROM processed_events WHERE consumer = 'aggregator'",
                    Integer.class)).isEqualTo(1);

            // ---- redeliver job 1's event: twice more through the real fan-out path -------------------------------
            String event = JSON.writeValueAsString(Map.of(
                    "eventId", "BatchCompleted:" + job1, "type", "BatchCompleted", "jobId", job1,
                    "totalRows", d.rows(), "goodRows", d.expectedGood(), "badRows", d.expectedBad(),
                    "completedAt", "redelivered"));
            String topic = sns.createTopic(b -> b.name("trip-events")).topicArn();
            sns.publish(b -> b.topicArn(topic).message(event));
            sns.publish(b -> b.topicArn(topic).message(event));

            // ---- job 2: the SAME trips uploaded again (overlap) --------------------------------------------------
            // A naive "add this job's counts" aggregator would double every number here.
            UUID job2 = upload(base(ctx), d.csv());
            awaitStatus(job2, "COMPLETED", Duration.ofMinutes(3));
            await().atMost(Duration.ofMinutes(1)).until(() -> JDBC.queryForObject(
                    "SELECT count(*) FROM processed_events WHERE consumer = 'aggregator'", Integer.class) == 2);
            await().atMost(Duration.ofMinutes(1)).until(() -> depth(sqs, "aggregator-queue") == 0
                    && depth(sqs, "notifier-queue") == 0);

            assertThat(stats()).as("stats equal the oracle: nothing doubled").isEqualTo(expected);
            assertThat(JDBC.queryForObject("SELECT count(*) FROM processed_events WHERE consumer = 'aggregator'",
                    Integer.class)).as("3 deliveries of event 1 + 1 of event 2 = 2 distinct events").isEqualTo(2);

            // ---- notifications: one per job, however many times the event arrived -------------------------------
            var notifications = drain(sqs, notifUrl, Duration.ofSeconds(6));
            assertThat(notifications).hasSize(2);
            assertThat(notifications).filteredOn(m -> m.body().contains(job1.toString())).hasSize(1);
            assertThat(notifications).filteredOn(m -> m.body().contains(job2.toString())).hasSize(1);
        }
    }

    private static SnsClient sns() {
        return SnsClient.builder().endpointOverride(LOCALSTACK.getEndpoint())
                .region(software.amazon.awssdk.regions.Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
    }

    /** Independent oracle: aggregate straight from the CSV (distinct valid trips, UTC day), no database involved. */
    private static Map<String, Stat> oracle(Path csv) throws Exception {
        Map<String, TripRow> trips = new TreeMap<>();
        for (String line : Files.readAllLines(csv).subList(1, (int) Files.lines(csv).count())) {
            var r = RowValidator.validate(line);
            if (r.valid()) {
                trips.put(r.trip().tripId(), r.trip());
            }
        }
        Map<String, Stat> out = new TreeMap<>();
        for (TripRow t : trips.values()) {
            String key = t.vehicleId() + "|" + t.startTime().withOffsetSameInstant(ZoneOffset.UTC).toLocalDate();
            out.merge(key, new Stat(1, t.distanceKm()), (a, b) -> new Stat(a.count() + 1, a.km().add(b.km())));
        }
        return out;
    }

    private static Map<String, Stat> stats() {
        Map<String, Stat> out = new TreeMap<>();
        JDBC.query("SELECT vehicle_id, day, trip_count, total_km FROM daily_vehicle_stats", rs -> {
            out.put(rs.getString(1) + "|" + rs.getDate(2).toLocalDate(),
                    new Stat(rs.getInt(3), rs.getBigDecimal(4).setScale(2)));
        });
        return out;
    }
}
