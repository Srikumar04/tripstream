package com.tripstream.jobs;

import com.tripstream.events.BatchCompletedPublisher;
import com.tripstream.state.ChunkLedger;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Race-free completion detection. Called by the splitter (after it records chunksTotal) and by every chunk
 * worker (after it records a chunk). Whoever observes chunksDone == chunksTotal finalises the job; because
 * both the observation (consistent read of an atomic counter) and the finalisation are idempotent, it does not
 * matter if several callers get there at once or if the call is repeated after a redelivery.
 */
@Service
public class JobCompletion {

    private static final Logger log = LoggerFactory.getLogger(JobCompletion.class);

    private final ChunkLedger ledger;
    private final JobRepository jobs;
    private final BatchCompletedPublisher publisher;

    public JobCompletion(ChunkLedger ledger, JobRepository jobs, BatchCompletedPublisher publisher) {
        this.ledger = ledger;
        this.jobs = jobs;
        this.publisher = publisher;
    }

    public boolean tryComplete(UUID jobId) {
        var progress = ledger.progress(jobId);
        if (progress.isEmpty() || !progress.get().allDone()) {
            return false;
        }
        jobs.markCompleted(jobId);
        if (!jobs.completionEventPublished(jobId)) {
            // At-least-once: a crash between publish and the flag below re-publishes; consumers dedupe on eventId.
            publisher.publish(jobs.find(jobId).orElseThrow());
            jobs.markCompletionEventPublished(jobId);
            log.info("jobId={} COMPLETED, BatchCompleted published", jobId);
        }
        return true;
    }
}
