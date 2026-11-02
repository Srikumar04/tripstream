--liquibase formatted sql

--changeset tripstream:007-processed-events
-- Cheap duplicate-event skip for consumers; written in the same transaction as the work it guards.
CREATE TABLE processed_events (
    consumer     VARCHAR(40) NOT NULL,
    event_id     VARCHAR(120) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

--changeset tripstream:008-trips-vehicle-day-index
-- Supports the aggregator's per-(vehicle, UTC day) recompute.
CREATE INDEX idx_trips_vehicle_day ON trips (vehicle_id, ((start_time AT TIME ZONE 'UTC')::date));
