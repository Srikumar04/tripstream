package com.tripstream.chunk;

import java.util.UUID;

/** One chunk-jobs message. startRow is the 1-based data-row number of the chunk's first row in the source file. */
public record ChunkMessage(UUID jobId, int chunkId, long startRow, int rowCount, String key) {

    public static String chunkKey(UUID jobId, int chunkId) {
        return "chunks/%s/%06d.csv".formatted(jobId, chunkId);
    }

    public String ledgerKey() {
        return jobId + "#" + chunkId;
    }
}
