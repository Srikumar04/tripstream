package com.tripstream.aggregator;

import com.tripstream.events.EventParser;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("aggregator")
class AggregatorListener {

    private final EventParser parser;
    private final StatsAggregator aggregator;

    AggregatorListener(EventParser parser, StatsAggregator aggregator) {
        this.parser = parser;
        this.aggregator = aggregator;
    }

    @SqsListener("${tripstream.queues.aggregator}")
    void onEvent(String body) throws Exception {
        parser.batchCompleted(body).ifPresent(aggregator::apply);
    }
}
