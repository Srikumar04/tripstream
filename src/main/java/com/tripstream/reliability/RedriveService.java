package com.tripstream.reliability;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Moves messages from a queue's DLQ back to the source queue (the operator's "I fixed the cause, try again").
 * Send happens before delete, so a crash in between duplicates a message instead of losing it; every consumer is
 * idempotent so that is harmless. The native alternative is `aws sqs start-message-move-task` on real AWS.
 */
@Service
@Profile("api")
public class RedriveService {

    public static final Set<String> QUEUES = Set.of("file-events", "chunk-jobs", "aggregator-queue", "notifier-queue");
    private static final Logger log = LoggerFactory.getLogger(RedriveService.class);

    private final SqsAsyncClient sqs;

    public RedriveService(SqsAsyncClient sqs) {
        this.sqs = sqs;
    }

    public int redrive(String queue, int max) {
        if (!QUEUES.contains(queue)) {
            throw new IllegalArgumentException("unknown queue " + queue);
        }
        String source = sqs.getQueueUrl(b -> b.queueName(queue)).join().queueUrl();
        String dlq = sqs.getQueueUrl(b -> b.queueName(queue + "-dlq")).join().queueUrl();
        int moved = 0;
        while (moved < max) {
            int want = Math.min(10, max - moved);
            var batch = sqs.receiveMessage(b -> b.queueUrl(dlq).maxNumberOfMessages(want).waitTimeSeconds(1)
                    .visibilityTimeout(60)).join().messages();
            if (batch.isEmpty()) {
                break;
            }
            for (Message m : batch) {
                sqs.sendMessage(b -> b.queueUrl(source).messageBody(m.body())).join();
                sqs.deleteMessage(b -> b.queueUrl(dlq).receiptHandle(m.receiptHandle())).join();
                moved++;
            }
        }
        log.info("redrove {} message(s) from {}-dlq to {}", moved, queue, queue);
        return moved;
    }
}
