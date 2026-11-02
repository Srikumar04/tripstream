package com.tripstream.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripstream.chunk.ChunkMessage;
import com.tripstream.chunk.ChunkRepository;
import com.tripstream.chunk.RowValidator;
import com.tripstream.jobs.JobCompletion;
import com.tripstream.jobs.JobRepository;
import com.tripstream.jobs.JobStatus;
import com.tripstream.state.ChunkLedger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Streams the uploaded CSV line by line (never holding more than one chunk in memory), writes each chunk to S3
 * and enqueues one chunk-jobs message per chunk.
 *
 * Idempotent / retry-safe: chunk boundaries depend only on the file contents and chunk size, chunk objects have
 * deterministic keys (overwritten with identical bytes), job_chunks inserts are ON CONFLICT DO NOTHING, and the
 * chunk-jobs message for a chunk may be sent more than once (workers dedupe on jobId#chunkId).
 */
@Service
@Profile("splitter")
public class Splitter {

    private static final Logger log = LoggerFactory.getLogger(Splitter.class);

    private final S3Client s3;
    private final SqsAsyncClient sqs;
    private final JobRepository jobs;
    private final ChunkRepository chunks;
    private final ChunkLedger ledger;
    private final JobCompletion completion;
    private final ObjectMapper mapper;
    private final String rawBucket;
    private final String chunksBucket;
    private final String chunkQueueName;
    private final int chunkRows;
    private volatile String chunkQueueUrl;

    public Splitter(S3Client s3, SqsAsyncClient sqs, JobRepository jobs, ChunkRepository chunks,
            ChunkLedger ledger, JobCompletion completion, ObjectMapper mapper,
            @Value("${tripstream.buckets.raw}") String rawBucket,
            @Value("${tripstream.buckets.chunks}") String chunksBucket,
            @Value("${tripstream.queues.chunk-jobs}") String chunkQueueName,
            @Value("${tripstream.splitter.chunk-rows:5000}") int chunkRows) {
        this.s3 = s3;
        this.sqs = sqs;
        this.jobs = jobs;
        this.chunks = chunks;
        this.ledger = ledger;
        this.completion = completion;
        this.mapper = mapper;
        this.rawBucket = rawBucket;
        this.chunksBucket = chunksBucket;
        this.chunkQueueName = chunkQueueName;
        this.chunkRows = chunkRows;
    }

    public void split(UUID jobId, String key) throws IOException {
        int chunkCount = 0;
        long rows = 0;
        try (var in = s3.getObject(b -> b.bucket(rawBucket).key(key));
                var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            if (header != null && !RowValidator.HEADER.equals(header.trim())) {
                log.warn("jobId={} unexpected header, failing job: {}", jobId, header);
                jobs.markFailed(jobId);
                return;
            }
            List<String> buffer = new ArrayList<>(chunkRows);
            long startRow = 1;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                buffer.add(line);
                rows++;
                if (buffer.size() == chunkRows) {
                    emit(jobId, chunkCount++, startRow, buffer);
                    buffer.clear();
                    startRow = rows + 1;
                }
            }
            if (!buffer.isEmpty()) {
                emit(jobId, chunkCount++, startRow, buffer);
            }
        }
        ledger.setTotal(jobId, chunkCount);
        jobs.markProcessing(jobId);
        log.info("jobId={} split into {} chunks, {} rows", jobId, chunkCount, rows);
        // Every chunk may already be done (tiny file, fast workers, or empty file): finalise from this side too.
        completion.tryComplete(jobId);
    }

    private void emit(UUID jobId, int chunkId, long startRow, List<String> lines) throws JsonProcessingException {
        String key = ChunkMessage.chunkKey(jobId, chunkId);
        String body = RowValidator.HEADER + "\n" + String.join("\n", lines) + "\n";
        s3.putObject(b -> b.bucket(chunksBucket).key(key), RequestBody.fromString(body, StandardCharsets.UTF_8));
        chunks.insertPending(jobId, chunkId, lines.size());
        String message = mapper.writeValueAsString(new ChunkMessage(jobId, chunkId, startRow, lines.size(), key));
        sqs.sendMessage(b -> b.queueUrl(queueUrl()).messageBody(message)).join();
    }

    private String queueUrl() {
        String u = chunkQueueUrl;
        if (u == null) {
            u = sqs.getQueueUrl(b -> b.queueName(chunkQueueName)).join().queueUrl();
            chunkQueueUrl = u;
        }
        return u;
    }

    /** True if a file event should (re)run the split: only before chunks are fully enqueued. */
    public boolean shouldSplit(UUID jobId) {
        return jobs.status(jobId).map(s -> s == JobStatus.RECEIVED).orElse(false);
    }
}
