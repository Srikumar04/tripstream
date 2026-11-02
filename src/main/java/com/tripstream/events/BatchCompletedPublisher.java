package com.tripstream.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripstream.jobs.JobView;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;

/** Publishes BatchCompleted to the trip-events topic. eventId is stable per job so consumers can dedupe. */
@Component
public class BatchCompletedPublisher {

    private final SnsClient sns;
    private final ObjectMapper mapper;
    private final String topicName;
    private volatile String topicArn;

    public BatchCompletedPublisher(SnsClient sns, ObjectMapper mapper,
            @Value("${tripstream.topics.trip-events}") String topicName) {
        this.sns = sns;
        this.mapper = mapper;
        this.topicName = topicName;
    }

    public void publish(JobView job) {
        String body;
        try {
            body = mapper.writeValueAsString(Map.of(
                    "eventId", "BatchCompleted:" + job.jobId(),
                    "type", "BatchCompleted",
                    "jobId", job.jobId(),
                    "totalRows", job.totalRows(),
                    "goodRows", job.goodRows(),
                    "badRows", job.badRows(),
                    "completedAt", String.valueOf(job.completedAt())));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        sns.publish(b -> b.topicArn(arn()).message(body));
    }

    private String arn() {
        String a = topicArn;
        if (a == null) {
            a = sns.createTopic(b -> b.name(topicName)).topicArn(); // idempotent: returns the existing ARN
            topicArn = a;
        }
        return a;
    }
}
