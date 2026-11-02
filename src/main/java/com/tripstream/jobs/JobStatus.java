package com.tripstream.jobs;

/** Job lifecycle. CREATED -> RECEIVED (file landed in S3); later phases add processing states. */
public enum JobStatus {
    CREATED,
    RECEIVED
}
