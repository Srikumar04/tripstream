package com.tripstream.aggregator;

import com.tripstream.events.BatchCompletedEvent;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Maintains daily_vehicle_stats. Idempotent by construction: for every (vehicle, UTC day) touched by the job it
 * <b>recomputes</b> the totals from the trips table and overwrites the row, rather than adding the job's numbers.
 * Consequences: a duplicate or reordered event cannot double-count, and overlapping uploads (a later job re-sending
 * trips that already exist) cannot double-count either.
 *
 * On top of that an event-id row in processed_events (same transaction) lets duplicates skip the work entirely, and
 * a transaction-scoped advisory lock serialises recomputes so two concurrent events cannot overwrite each other
 * with a stale snapshot.
 */
@Service
@Profile("aggregator")
public class StatsAggregator {

    private static final Logger log = LoggerFactory.getLogger(StatsAggregator.class);
    private static final long LOCK_ID = 7001;
    private static final String DAY = "(t.start_time AT TIME ZONE 'UTC')::date";

    private static final String RECOMPUTE = """
            WITH affected AS (
                SELECT DISTINCT t.vehicle_id, %1$s AS day FROM trips t WHERE t.job_id = ?
            )
            INSERT INTO daily_vehicle_stats (vehicle_id, day, trip_count, total_km)
            SELECT t.vehicle_id, a.day, count(*), sum(t.distance_km)
            FROM trips t JOIN affected a ON a.vehicle_id = t.vehicle_id AND a.day = %1$s
            GROUP BY t.vehicle_id, a.day
            ON CONFLICT (vehicle_id, day) DO UPDATE
                SET trip_count = EXCLUDED.trip_count, total_km = EXCLUDED.total_km
            """.formatted(DAY);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public StatsAggregator(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    /** @return true if this call applied the event, false if it was a duplicate */
    public boolean apply(BatchCompletedEvent event) {
        UUID jobId = event.jobId();
        Boolean applied = tx.execute(status -> {
            jdbc.execute("SELECT pg_advisory_xact_lock(" + LOCK_ID + ")");
            int fresh = jdbc.update("INSERT INTO processed_events (consumer, event_id) VALUES ('aggregator', ?) "
                    + "ON CONFLICT DO NOTHING", event.eventId());
            if (fresh == 0) {
                return false;
            }
            jdbc.update(RECOMPUTE, jobId);
            return true;
        });
        log.info("jobId={} eventId={} {}", jobId, event.eventId(),
                Boolean.TRUE.equals(applied) ? "stats recomputed" : "duplicate event skipped");
        return Boolean.TRUE.equals(applied);
    }
}
