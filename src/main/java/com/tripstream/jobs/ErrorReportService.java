package com.tripstream.jobs;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** Lists the per-chunk error-report objects for a job and hands out short-lived download links. */
@Service
@Profile("api")
public class ErrorReportService {

    public record ErrorFile(String key, String url) {
    }

    private final S3Client s3;
    private final S3Presigner presigner;
    private final String errorsBucket;

    public ErrorReportService(S3Client s3, S3Presigner presigner,
            @Value("${tripstream.buckets.errors}") String errorsBucket) {
        this.s3 = s3;
        this.presigner = presigner;
        this.errorsBucket = errorsBucket;
    }

    public List<ErrorFile> files(UUID jobId) {
        return s3.listObjectsV2Paginator(b -> b.bucket(errorsBucket).prefix(jobId + "/")).contents().stream()
                .map(S3Object::key)
                .sorted()
                .map(key -> new ErrorFile(key, presigner.presignGetObject(r -> r
                        .signatureDuration(Duration.ofMinutes(15))
                        .getObjectRequest(GetObjectRequest.builder().bucket(errorsBucket).key(key).build()))
                        .url().toString()))
                .toList();
    }
}
