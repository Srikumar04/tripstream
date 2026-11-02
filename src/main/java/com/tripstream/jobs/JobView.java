package com.tripstream.jobs;

import java.time.Instant;
import java.util.UUID;

public record JobView(
        UUID jobId,
        String status,
        long totalRows,
        long goodRows,
        long badRows,
        Instant createdAt,
        Instant completedAt,
        String errorReport) {

    public JobView withErrorReport(String link) {
        return new JobView(jobId, status, totalRows, goodRows, badRows, createdAt, completedAt, link);
    }
}
