package com.tripstream.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripstream.jobs.JobRepository;
import io.awspring.cloud.sqs.annotation.SqsListener;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Consumes S3 ObjectCreated events from file-events and moves the job to RECEIVED.
 * Phase 2 extends this to hand the file to the splitter.
 *
 * Idempotent: duplicate events are no-ops. Events we can never process (unparseable, unknown job)
 * are logged and acknowledged rather than retried, so they cannot become poison messages.
 */
@Component
@Profile("splitter")
class FileEventListener {

    private static final Logger log = LoggerFactory.getLogger(FileEventListener.class);

    private final JobRepository jobs;
    private final ObjectMapper mapper;

    FileEventListener(JobRepository jobs, ObjectMapper mapper) {
        this.jobs = jobs;
        this.mapper = mapper;
    }

    @SqsListener("${tripstream.queues.file-events}")
    void onFileEvent(String body) throws Exception {
        JsonNode records = mapper.readTree(body).path("Records");
        if (!records.isArray() || records.isEmpty()) {
            log.info("Ignoring non-record S3 message (e.g. s3:TestEvent)");
            return;
        }
        for (JsonNode record : records) {
            String key = URLDecoder.decode(record.path("s3").path("object").path("key").asText(),
                    StandardCharsets.UTF_8);
            handleKey(key);
        }
    }

    private void handleKey(String key) {
        UUID jobId = parseJobId(key);
        if (jobId == null) {
            log.warn("Ignoring object with unexpected key {}", key);
            return;
        }
        if (jobs.markReceived(jobId)) {
            log.info("jobId={} RECEIVED key={}", jobId, key);
        } else if (jobs.exists(jobId)) {
            log.info("jobId={} duplicate file event, already past CREATED", jobId);
        } else {
            log.warn("jobId={} file event for unknown job, ignoring", jobId);
        }
    }

    /** Expects uploads/{jobId}/input.csv */
    static UUID parseJobId(String key) {
        String[] parts = key.split("/");
        if (parts.length != 3 || !parts[0].equals("uploads")) {
            return null;
        }
        try {
            return UUID.fromString(parts[1]);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
