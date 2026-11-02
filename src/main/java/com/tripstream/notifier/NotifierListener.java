package com.tripstream.notifier;

import com.tripstream.events.BatchCompletedEvent;
import com.tripstream.events.EventParser;
import com.tripstream.state.ChunkLedger;
import io.awspring.cloud.sqs.annotation.SqsListener;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Turns BatchCompleted events into notifications. A duplicate event must not notify twice, so each event id is
 * claimed in DynamoDB (same claim/lease pattern as chunks) before sending and marked done after.
 * Delivery is at-least-once: a crash between send and mark-done can notify again after the lease expires.
 */
@Component
@Profile("notifier")
class NotifierListener {

    private static final Logger log = LoggerFactory.getLogger(NotifierListener.class);
    private static final Duration LEASE = Duration.ofSeconds(60);

    private final EventParser parser;
    private final ChunkLedger ledger;
    private final NotificationSender sender;

    NotifierListener(EventParser parser, ChunkLedger ledger, NotificationSender sender) {
        this.parser = parser;
        this.ledger = ledger;
        this.sender = sender;
    }

    @SqsListener("${tripstream.queues.notifier}")
    void onEvent(String body) throws Exception {
        var event = parser.batchCompleted(body);
        if (event.isPresent()) {
            handle(event.get());
        }
    }

    private void handle(BatchCompletedEvent e) {
        String pk = "notify#" + e.eventId();
        var claim = ledger.claim(pk, LEASE);
        switch (claim.result()) {
            case ALREADY_DONE -> log.info("jobId={} notification already sent, skipping", e.jobId());
            case BUSY -> throw new IllegalStateException("notification for " + e.eventId() + " in progress elsewhere");
            case CLAIMED -> {
                try {
                    sender.send(describe(e));
                    ledger.markDone(pk);
                } catch (RuntimeException ex) {
                    ledger.release(pk, claim.owner());
                    throw ex;
                }
            }
        }
    }

    static Notification describe(BatchCompletedEvent e) {
        String level = e.badRows() > 0 ? "WARN" : "INFO";
        return new Notification(level, "Trip file processed: " + e.jobId(),
                "jobId=%s total=%d good=%d bad=%d%s".formatted(e.jobId(), e.totalRows(), e.goodRows(), e.badRows(),
                        e.badRows() > 0 ? " (see /jobs/" + e.jobId() + "/errors)" : ""));
    }
}
