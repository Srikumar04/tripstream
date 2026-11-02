package com.tripstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/** Runs the real init script (the same one docker compose uses) and verifies the resources it creates. */
@Testcontainers
class LocalStackInitScriptIT {

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

    private static StaticCredentialsProvider creds() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
    }

    private static URI endpoint() {
        return LOCALSTACK.getEndpoint();
    }

    @Test
    void createsQueuesWithDlqRedrivePolicy() {
        try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(creds()).build()) {
            var names = sqs.listQueues().queueUrls();
            for (String q : new String[] {"file-events", "chunk-jobs", "aggregator-queue", "notifier-queue"}) {
                assertThat(names).anyMatch(u -> u.endsWith("/" + q));
                assertThat(names).anyMatch(u -> u.endsWith("/" + q + "-dlq"));
                String url = sqs.getQueueUrl(b -> b.queueName(q)).queueUrl();
                var redrive = sqs.getQueueAttributes(b -> b.queueUrl(url)
                        .attributeNames(QueueAttributeName.REDRIVE_POLICY)).attributes();
                assertThat(redrive.get(QueueAttributeName.REDRIVE_POLICY))
                        .contains(q + "-dlq")
                        .containsPattern("\"maxReceiveCount\":\"?5\"?");
            }
        }
    }

    @Test
    void createsBucketsTopicAndTables() {
        try (S3Client s3 = S3Client.builder().endpointOverride(endpoint()).region(Region.US_EAST_1)
                        .credentialsProvider(creds()).forcePathStyle(true).build();
                SnsClient sns = SnsClient.builder().endpointOverride(endpoint()).region(Region.US_EAST_1)
                        .credentialsProvider(creds()).build();
                DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint())
                        .region(Region.US_EAST_1).credentialsProvider(creds()).build()) {
            assertThat(s3.listBuckets().buckets()).extracting(b -> b.name())
                    .contains("tripstream-raw", "tripstream-chunks", "tripstream-errors");
            assertThat(sns.listTopics().topics()).anyMatch(t -> t.topicArn().endsWith(":trip-events"));
            assertThat(ddb.listTables().tableNames())
                    .contains("tripstream-idempotency", "tripstream-job-progress");
        }
    }
}
