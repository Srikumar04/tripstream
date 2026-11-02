--liquibase formatted sql

--changeset tripstream:001-jobs
CREATE TABLE jobs (
    job_id       UUID PRIMARY KEY,
    status       VARCHAR(20) NOT NULL,
    total_rows   BIGINT NOT NULL DEFAULT 0,
    good_rows    BIGINT NOT NULL DEFAULT 0,
    bad_rows     BIGINT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

--changeset tripstream:002-job-chunks
CREATE TABLE job_chunks (
    job_id      UUID NOT NULL REFERENCES jobs(job_id),
    chunk_id    INT NOT NULL,
    status      VARCHAR(20) NOT NULL,
    attempts    INT NOT NULL DEFAULT 0,
    row_count   INT NOT NULL DEFAULT 0,
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, chunk_id)
);

--changeset tripstream:003-trips
-- trip_id is the natural key: upserts (INSERT ... ON CONFLICT) make chunk reprocessing idempotent.
CREATE TABLE trips (
    trip_id     VARCHAR(64) PRIMARY KEY,
    vehicle_id  VARCHAR(64) NOT NULL,
    driver_id   VARCHAR(64) NOT NULL,
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ NOT NULL,
    distance_km NUMERIC(10,2) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    job_id      UUID NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_trips_vehicle_start ON trips (vehicle_id, start_time);
CREATE INDEX idx_trips_job ON trips (job_id);

--changeset tripstream:004-daily-vehicle-stats
CREATE TABLE daily_vehicle_stats (
    vehicle_id VARCHAR(64) NOT NULL,
    day        DATE NOT NULL,
    trip_count BIGINT NOT NULL DEFAULT 0,
    total_km   NUMERIC(14,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (vehicle_id, day)
);
