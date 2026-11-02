package com.tripstream.chunk;

import com.tripstream.jobs.JobCompletion;
import com.tripstream.reliability.Heartbeat;
import io.awspring.cloud.sqs.listener.Visibility;
import com.tripstream.state.ChunkLedger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Processes one chunk. Safe to run any number of times for the same chunk:
 * <ol>
 *   <li>claim jobId#chunkId in DynamoDB (conditional put with lease); DONE means skip, held by someone else means retry later</li>
 *   <li>parse and validate; bad rows go to a deterministic error-report object (overwrite is idempotent)</li>
 *   <li>one DB transaction: batch upsert good rows (natural key) + mark the chunk row done</li>
 *   <li>one DynamoDB transaction: mark DONE + increment chunksDone (at most once per chunk)</li>
 *   <li>check for job completion (also done when we skip a DONE chunk, covering a crash after step 4)</li>
 * </ol>
 */
@Service
@Profile("worker")
public class ChunkProcessor {

    /** Thrown when another worker holds a live claim; the message is left to reappear after the visibility timeout. */
    public static class ChunkBusyException extends RuntimeException {
        ChunkBusyException(String message) {
            super(message);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ChunkProcessor.class);

    private final ChunkLedger ledger;
    private final ChunkRepository chunks;
    private final TripWriter writer;
    private final JobCompletion completion;
    private final S3Client s3;
    private final TransactionTemplate tx;
    private final String chunksBucket;
    private final String errorsBucket;
    private final Duration lease;
    private final Duration heartbeatInterval;
    private final int visibilitySeconds;
    private final Duration chaosDelay;
    private final boolean commitPerBatch;

    public ChunkProcessor(ChunkLedger ledger, ChunkRepository chunks, TripWriter writer, JobCompletion completion,
            S3Client s3, TransactionTemplate tx,
            @Value("${tripstream.buckets.chunks}") String chunksBucket,
            @Value("${tripstream.buckets.errors}") String errorsBucket,
            @Value("${tripstream.worker.lease:PT90S}") Duration lease,
            @Value("${tripstream.reliability.heartbeat-interval:PT30S}") Duration heartbeatInterval,
            @Value("${tripstream.reliability.visibility-timeout:PT2M}") Duration visibilityTimeout,
            @Value("${tripstream.chaos.chunk-delay:PT0S}") Duration chaosDelay,
            @Value("${tripstream.worker.commit-per-batch:false}") boolean commitPerBatch) {
        this.ledger = ledger;
        this.chunks = chunks;
        this.writer = writer;
        this.completion = completion;
        this.s3 = s3;
        this.tx = tx;
        this.chunksBucket = chunksBucket;
        this.errorsBucket = errorsBucket;
        this.lease = lease;
        this.heartbeatInterval = heartbeatInterval;
        this.visibilitySeconds = (int) visibilityTimeout.toSeconds();
        this.chaosDelay = chaosDelay;
        this.commitPerBatch = commitPerBatch;
    }

    public void process(ChunkMessage msg, Visibility visibility) throws IOException {
        String pk = msg.ledgerKey();
        var claim = ledger.claim(pk, lease);
        switch (claim.result()) {
            case ALREADY_DONE -> {
                log.info("jobId={} chunkId={} already done, skipping", msg.jobId(), msg.chunkId());
                completion.tryComplete(msg.jobId());
                return;
            }
            case BUSY -> throw new ChunkBusyException("chunk " + pk + " is being processed by another worker");
            case CLAIMED -> {
            }
        }
        try (Heartbeat ignored = Heartbeat.start(heartbeatInterval, () -> beat(msg, pk, claim.owner(), visibility))) {
            chunks.markStarted(msg.jobId(), msg.chunkId()); // attempts = number of times a worker began this chunk
            chaosPause();
            run(msg, pk);
        } catch (RuntimeException | IOException e) {
            ledger.release(pk, claim.owner());
            throw e;
        }
    }

    /** Keeps a slow chunk from being redelivered: extend the message's visibility and our claim lease. */
    private void beat(ChunkMessage msg, String pk, String owner, Visibility visibility) {
        if (!ledger.renewLease(pk, owner, lease)) {
            log.warn("jobId={} chunkId={} lost its claim lease; another worker may take over", msg.jobId(), msg.chunkId());
        }
        visibility.changeTo(visibilitySeconds);
    }

    /** Test/chaos hook (tripstream.chaos.chunk-delay): simulates a slow chunk so failures can be injected mid-flight. */
    private void chaosPause() {
        if (chaosDelay.isZero()) {
            return;
        }
        try {
            Thread.sleep(chaosDelay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run(ChunkMessage msg, String pk) throws IOException {
        // Dedupe by trip_id within the chunk, iterated in trip_id order: every worker then takes row locks in the
        // same global order, which prevents deadlocks between chunks that share (duplicated) trip_ids.
        Map<String, TripRow> good = new TreeMap<>();
        List<String> errorLines = new ArrayList<>();
        int validRows = 0;
        int badRows = 0;
        long rowNumber = msg.startRow() - 1;
        try (var in = s3.getObject(b -> b.bucket(chunksBucket).key(msg.key()));
                var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            reader.readLine(); // header
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                rowNumber++;
                var result = RowValidator.validate(line);
                if (result.valid()) {
                    validRows++;
                    good.put(result.trip().tripId(), result.trip());
                } else {
                    badRows++;
                    errorLines.add(rowNumber + "," + csv(result.error()) + "," + csv(line));
                }
            }
        }

        if (badRows > 0) {
            String body = "row_number,reason,raw_line\n" + String.join("\n", errorLines) + "\n";
            String errorKey = "%s/chunk-%06d.csv".formatted(msg.jobId(), msg.chunkId());
            s3.putObject(b -> b.bucket(errorsBucket).key(errorKey),
                    RequestBody.fromString(body, StandardCharsets.UTF_8));
        }

        final int valid = validRows;
        final int bad = badRows;
        if (commitPerBatch) {
            // Each JDBC batch commits on its own, so row locks are held for milliseconds instead of the whole chunk.
            // Safe because the upsert is idempotent: a crash mid-chunk leaves some rows written, and the retry
            // rewrites them as no-ops. The chunk is only marked done after every batch is in.
            writer.upsert(msg.jobId(), new ArrayList<>(good.values()));
            chunks.markDone(msg.jobId(), msg.chunkId(), valid, bad);
        } else {
            tx.executeWithoutResult(status -> {
                writer.upsert(msg.jobId(), new ArrayList<>(good.values()));
                chunks.markDone(msg.jobId(), msg.chunkId(), valid, bad);
            });
        }

        boolean counted = ledger.completeChunk(pk, msg.jobId());
        log.info("jobId={} chunkId={} done valid={} bad={} counted={}", msg.jobId(), msg.chunkId(), valid, bad, counted);
        completion.tryComplete(msg.jobId());
    }

    private static String csv(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
}
