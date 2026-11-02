package com.tripstream.jobs;

/** CREATED -> RECEIVED (file landed) -> PROCESSING (chunks enqueued) -> COMPLETED | FAILED. */
public enum JobStatus {
    CREATED,
    RECEIVED,
    PROCESSING,
    COMPLETED,
    FAILED
}
