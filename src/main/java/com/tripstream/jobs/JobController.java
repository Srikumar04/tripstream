package com.tripstream.jobs;

import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("api")
class JobController {

    private final JobRepository jobs;
    private final ErrorReportService errors;

    JobController(JobRepository jobs, ErrorReportService errors) {
        this.jobs = jobs;
        this.errors = errors;
    }

    @GetMapping("/jobs/{id}")
    JobView get(@PathVariable String id) {
        UUID jobId = parse(id);
        JobView job = jobs.find(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "job not found"));
        return job.badRows() > 0 ? job.withErrorReport("/jobs/" + jobId + "/errors") : job;
    }

    /** Presigned download links for the per-chunk error reports (CSV: row_number,reason,raw_line). */
    @GetMapping("/jobs/{id}/errors")
    java.util.Map<String, Object> errors(@PathVariable String id) {
        UUID jobId = parse(id);
        if (!jobs.exists(jobId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "job not found");
        }
        return java.util.Map.of("jobId", jobId, "files", errors.files(jobId));
    }

    private static UUID parse(String id) {
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "jobId must be a UUID");
        }
    }
}
