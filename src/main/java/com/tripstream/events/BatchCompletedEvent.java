package com.tripstream.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.UUID;

/** Published to the trip-events topic when every chunk of a job is done. eventId is stable per job (dedupe key). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BatchCompletedEvent(String eventId, String type, UUID jobId, long totalRows, long goodRows,
        long badRows, String completedAt) {

    public static final String TYPE = "BatchCompleted";
}
