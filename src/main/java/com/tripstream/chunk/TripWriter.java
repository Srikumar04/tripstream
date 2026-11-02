package com.tripstream.chunk;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Batch upsert keyed on the natural key trip_id; safe to run any number of times for the same rows. */
@Component
public class TripWriter {

    // The WHERE clause skips no-op updates, so reprocessing identical rows writes nothing (no bloat,
    // updated_at stays put).
    private static final String UPSERT = """
            INSERT INTO trips (trip_id, vehicle_id, driver_id, start_time, end_time, distance_km, status, job_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (trip_id) DO UPDATE SET
                vehicle_id = EXCLUDED.vehicle_id, driver_id = EXCLUDED.driver_id,
                start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
                distance_km = EXCLUDED.distance_km, status = EXCLUDED.status,
                job_id = EXCLUDED.job_id, updated_at = now()
            WHERE (trips.vehicle_id, trips.driver_id, trips.start_time, trips.end_time,
                   trips.distance_km, trips.status, trips.job_id)
                  IS DISTINCT FROM (EXCLUDED.vehicle_id, EXCLUDED.driver_id, EXCLUDED.start_time,
                   EXCLUDED.end_time, EXCLUDED.distance_km, EXCLUDED.status, EXCLUDED.job_id)
            """;

    private final JdbcTemplate jdbc;
    private final int batchSize;

    public TripWriter(JdbcTemplate jdbc, @Value("${tripstream.worker.db-batch-size:500}") int batchSize) {
        this.jdbc = jdbc;
        this.batchSize = batchSize;
    }

    /** rows must have distinct tripIds (one statement cannot upsert the same key twice). */
    public void upsert(UUID jobId, List<TripRow> rows) {
        jdbc.batchUpdate(UPSERT, rows, batchSize, (PreparedStatement ps, TripRow r) -> bind(ps, jobId, r));
    }

    private static void bind(PreparedStatement ps, UUID jobId, TripRow r) throws SQLException {
        ps.setString(1, r.tripId());
        ps.setString(2, r.vehicleId());
        ps.setString(3, r.driverId());
        ps.setObject(4, r.startTime());
        ps.setObject(5, r.endTime());
        ps.setBigDecimal(6, r.distanceKm());
        ps.setString(7, r.status());
        ps.setObject(8, jobId);
    }
}
