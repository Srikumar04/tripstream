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

    JobController(JobRepository jobs) {
        this.jobs = jobs;
    }

    @GetMapping("/jobs/{id}")
    JobView get(@PathVariable String id) {
        UUID jobId;
        try {
            jobId = UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "jobId must be a UUID");
        }
        return jobs.find(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "job not found"));
    }
}
