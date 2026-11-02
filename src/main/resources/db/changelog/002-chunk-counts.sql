--liquibase formatted sql

--changeset tripstream:005-chunk-row-counts
-- valid_rows counts valid rows in the chunk (duplicates included); bad_rows feeds the error report.
ALTER TABLE job_chunks ADD COLUMN valid_rows INT NOT NULL DEFAULT 0;
ALTER TABLE job_chunks ADD COLUMN bad_rows INT NOT NULL DEFAULT 0;

--changeset tripstream:006-completion-event-flag
-- Set after BatchCompleted is published, so redelivered "last chunk" messages do not re-publish.
ALTER TABLE jobs ADD COLUMN completion_event_published_at TIMESTAMPTZ;
