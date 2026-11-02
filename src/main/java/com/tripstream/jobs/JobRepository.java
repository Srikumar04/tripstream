package com.tripstream.jobs;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobRepository {

    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void create(UUID jobId) {
        jdbc.update("INSERT INTO jobs (job_id, status) VALUES (?, ?)", jobId, JobStatus.CREATED.name());
    }

    /**
     * CREATED -> RECEIVED. Idempotent: a duplicate S3 event matches zero rows and changes nothing.
     * @return true if this call performed the transition
     */
    public boolean markReceived(UUID jobId) {
        return jdbc.update("UPDATE jobs SET status = ? WHERE job_id = ? AND status = ?",
                JobStatus.RECEIVED.name(), jobId, JobStatus.CREATED.name()) == 1;
    }

    public boolean exists(UUID jobId) {
        Integer n = jdbc.queryForObject("SELECT count(*) FROM jobs WHERE job_id = ?", Integer.class, jobId);
        return n != null && n > 0;
    }

    public Optional<JobView> find(UUID jobId) {
        return jdbc.query(
                "SELECT job_id, status, total_rows, good_rows, bad_rows, created_at, completed_at "
                        + "FROM jobs WHERE job_id = ?",
                rs -> {
                    if (!rs.next()) {
                        return Optional.<JobView>empty();
                    }
                    Timestamp completed = rs.getTimestamp("completed_at");
                    return Optional.of(new JobView(
                            rs.getObject("job_id", UUID.class),
                            rs.getString("status"),
                            rs.getLong("total_rows"),
                            rs.getLong("good_rows"),
                            rs.getLong("bad_rows"),
                            rs.getTimestamp("created_at").toInstant(),
                            completed == null ? null : completed.toInstant(),
                            null));
                },
                jobId);
    }

    public Optional<JobStatus> status(UUID jobId) {
        return jdbc.query("SELECT status FROM jobs WHERE job_id = ?",
                rs -> rs.next() ? Optional.of(JobStatus.valueOf(rs.getString(1))) : Optional.<JobStatus>empty(), jobId);
    }

    /** RECEIVED -> PROCESSING once every chunk is enqueued. Guarded so it never reverts COMPLETED. */
    public void markProcessing(UUID jobId) {
        jdbc.update("UPDATE jobs SET status = ? WHERE job_id = ? AND status IN (?, ?)",
                JobStatus.PROCESSING.name(), jobId, JobStatus.RECEIVED.name(), JobStatus.PROCESSING.name());
    }

    public void markFailed(UUID jobId) {
        jdbc.update("UPDATE jobs SET status = ?, completed_at = COALESCE(completed_at, now()) "
                + "WHERE job_id = ? AND status <> ?", JobStatus.FAILED.name(), jobId, JobStatus.COMPLETED.name());
    }

    /**
     * Idempotent: recomputes totals from durable state, so running it twice yields identical values.
     * good_rows = distinct trips stored for this job (duplicates collapse onto one row via upsert).
     */
    public void markCompleted(UUID jobId) {
        jdbc.update("""
                UPDATE jobs j SET status = 'COMPLETED',
                       completed_at = COALESCE(j.completed_at, now()),
                       total_rows = s.total,
                       bad_rows = s.bad,
                       good_rows = (SELECT count(*) FROM trips t WHERE t.job_id = j.job_id)
                FROM (SELECT COALESCE(sum(row_count), 0) AS total, COALESCE(sum(bad_rows), 0) AS bad
                      FROM job_chunks WHERE job_id = ?) s
                WHERE j.job_id = ?
                """, jobId, jobId);
    }

    public boolean completionEventPublished(UUID jobId) {
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE job_id = ? AND completion_event_published_at IS NOT NULL",
                Integer.class, jobId);
        return n != null && n > 0;
    }

    public void markCompletionEventPublished(UUID jobId) {
        jdbc.update("UPDATE jobs SET completion_event_published_at = COALESCE(completion_event_published_at, now()) "
                + "WHERE job_id = ?", jobId);
    }
}
