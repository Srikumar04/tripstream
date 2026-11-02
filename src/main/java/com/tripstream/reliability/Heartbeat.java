package com.tripstream.reliability;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a keep-alive action on a fixed interval until closed. Used to extend the SQS visibility timeout (and the
 * DynamoDB lease) while a long chunk or split is running, so it is not redelivered to another worker mid-flight.
 * Closing it (including when the owning thread dies) stops the extensions: a crashed worker stops renewing and
 * its message becomes visible again after the current timeout.
 */
public final class Heartbeat implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);
    private static final ScheduledExecutorService POOL = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "heartbeat");
        t.setDaemon(true);
        return t;
    });

    private final ScheduledFuture<?> future;

    private Heartbeat(ScheduledFuture<?> future) {
        this.future = future;
    }

    /** A zero interval disables the heartbeat. */
    public static Heartbeat start(Duration interval, Runnable beat) {
        if (interval.isZero() || interval.isNegative()) {
            return new Heartbeat(null);
        }
        long ms = interval.toMillis();
        return new Heartbeat(POOL.scheduleAtFixedRate(() -> {
            try {
                beat.run();
            } catch (RuntimeException e) {
                // must not escape: an exception would cancel all future beats
                log.warn("heartbeat failed: {}", e.toString());
            }
        }, ms, ms, TimeUnit.MILLISECONDS));
    }

    @Override
    public void close() {
        if (future != null) {
            future.cancel(false);
        }
    }
}
