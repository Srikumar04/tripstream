package com.tripstream.chunk;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ChunkRepository {

    private final JdbcTemplate jdbc;

    public ChunkRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Re-splitting never resets a chunk that already finished. */
    public void insertPending(UUID jobId, int chunkId, int rowCount) {
        jdbc.update("INSERT INTO job_chunks (job_id, chunk_id, status, row_count) VALUES (?, ?, 'PENDING', ?) "
                + "ON CONFLICT (job_id, chunk_id) DO NOTHING", jobId, chunkId, rowCount);
    }

    public void markStarted(UUID jobId, int chunkId) {
        jdbc.update("UPDATE job_chunks SET status = 'PROCESSING', attempts = attempts + 1, "
                + "started_at = COALESCE(started_at, now()) WHERE job_id = ? AND chunk_id = ?", jobId, chunkId);
    }

    /** Called inside the same DB transaction as the trip upserts. */
    public void markDone(UUID jobId, int chunkId, int validRows, int badRows) {
        jdbc.update("UPDATE job_chunks SET status = 'DONE', valid_rows = ?, bad_rows = ?, finished_at = now() "
                + "WHERE job_id = ? AND chunk_id = ?", validRows, badRows, jobId, chunkId);
    }
}
