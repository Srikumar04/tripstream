package com.tripstream.reliability;

import com.tripstream.chunk.ChunkProcessor.ChunkBusyException;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * Retry policy for every listener. A failed message is not acknowledged; here we set how long it stays invisible
 * before SQS redelivers it: exponential backoff (base * 2^(receives-1), capped, +/-20% jitter) so a failing
 * dependency is not hammered. After the queue's maxReceiveCount (5) SQS moves the message to the DLQ.
 *
 * Rethrowing is essential, see handle(). A "busy" rejection (another worker holds a live claim on the same chunk) is not a failure of this message, so it
 * is retried after the claim lease instead of climbing the backoff curve towards the DLQ.
 */
@Component
class RetryBackoffErrorHandler implements ErrorHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(RetryBackoffErrorHandler.class);

    private final Duration base;
    private final Duration max;
    private final Duration busyDelay;

    RetryBackoffErrorHandler(
            @Value("${tripstream.reliability.retry-base-delay:PT2S}") Duration base,
            @Value("${tripstream.reliability.retry-max-delay:PT5M}") Duration max,
            @Value("${tripstream.worker.lease:PT90S}") Duration lease) {
        this.base = base;
        this.max = max;
        this.busyDelay = lease;
    }

    @Override
    public void handle(Message<Object> message, Throwable t) {
        int receives = receiveCount(message);
        boolean busy = isBusy(t);
        long seconds = busy ? busyDelay.toSeconds() : backoffSeconds(receives);
        Visibility visibility = message.getHeaders().get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class);
        if (visibility != null) {
            visibility.changeTo((int) Math.max(1, seconds));
        }
        Throwable root = rootCause(t);
        log.warn("{} (receive #{}); retrying in {}s: {}", busy ? "chunk busy" : "message failed", receives, seconds,
                root.toString());
        // Rethrow on purpose: an error handler that returns normally means "handled", and the framework would then
        // ACKNOWLEDGE (delete) the message. Throwing keeps it unacknowledged so SQS redelivers it after the delay
        // we just set, and eventually moves it to the DLQ.
        throw t instanceof RuntimeException re ? re : new IllegalStateException(t);
    }

    long backoffSeconds(int receives) {
        double exp = base.toMillis() / 1000.0 * Math.pow(2, Math.max(0, receives - 1));
        double capped = Math.min(exp, max.toMillis() / 1000.0);
        double jitter = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4;
        return Math.max(1, Math.round(capped * jitter));
    }

    private static int receiveCount(Message<?> m) {
        Object v = m.getHeaders().get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT);
        try {
            return v == null ? 1 : Integer.parseInt(v.toString());
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private static boolean isBusy(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ChunkBusyException) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c;
    }
}
