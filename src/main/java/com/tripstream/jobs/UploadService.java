package com.tripstream.jobs;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@Service
@Profile("api")
public class UploadService {

    /** S3 key layout: the jobId is recoverable from the key, so S3 events need no extra lookup. */
    public static String rawKey(UUID jobId) {
        return "uploads/" + jobId + "/input.csv";
    }

    public record Upload(UUID jobId, String key, String uploadUrl, Instant expiresAt) {
    }

    private final JobRepository jobs;
    private final S3Presigner presigner;
    private final String rawBucket;
    private final Duration expiry;

    public UploadService(JobRepository jobs, S3Presigner presigner,
            @Value("${tripstream.buckets.raw}") String rawBucket,
            @Value("${tripstream.upload.url-expiry:PT15M}") Duration expiry) {
        this.jobs = jobs;
        this.presigner = presigner;
        this.rawBucket = rawBucket;
        this.expiry = expiry;
    }

    public Upload createUpload() {
        UUID jobId = UUID.randomUUID();
        jobs.create(jobId);
        String key = rawKey(jobId);
        var presigned = presigner.presignPutObject(r -> r
                .signatureDuration(expiry)
                .putObjectRequest(PutObjectRequest.builder().bucket(rawBucket).key(key).build()));
        return new Upload(jobId, key, presigned.url().toString(), presigned.expiration());
    }
}
