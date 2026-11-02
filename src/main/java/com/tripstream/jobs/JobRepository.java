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
                            completed == null ? null : completed.toInstant()));
                },
                jobId);
    }
}
