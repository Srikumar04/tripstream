package com.tripstream.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class EventParser {

    private final ObjectMapper mapper;

    public EventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @return the event if the message is a BatchCompleted; empty for other (future) event types, which consumers
     *         acknowledge and ignore. Malformed JSON throws, so a poison message retries and lands in the DLQ.
     */
    public Optional<BatchCompletedEvent> batchCompleted(String body) throws JsonProcessingException {
        BatchCompletedEvent e = mapper.readValue(body, BatchCompletedEvent.class);
        return BatchCompletedEvent.TYPE.equals(e.type()) && e.jobId() != null && e.eventId() != null
                ? Optional.of(e) : Optional.empty();
    }
}
