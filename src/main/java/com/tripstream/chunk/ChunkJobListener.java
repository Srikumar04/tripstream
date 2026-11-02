package com.tripstream.chunk;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("worker")
class ChunkJobListener {

    private final ChunkProcessor processor;
    private final ObjectMapper mapper;

    ChunkJobListener(ChunkProcessor processor, ObjectMapper mapper) {
        this.processor = processor;
        this.mapper = mapper;
    }

    /** Concurrency is the backpressure knob: at most this many chunks in flight per worker process. */
    @SqsListener(value = "${tripstream.queues.chunk-jobs}",
            maxConcurrentMessages = "${tripstream.worker.concurrency:4}",
            maxMessagesPerPoll = "${tripstream.worker.poll-size:4}")
    void onChunk(String body) throws Exception {
        processor.process(mapper.readValue(body, ChunkMessage.class));
    }
}
