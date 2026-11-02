package com.tripstream.notifier;

/** Port for outbound notifications. Adapters: console (local default) and SNS (real, also usable with SES via SNS). */
public interface NotificationSender {

    /** Must either deliver or throw; the notifier retries on failure. */
    void send(Notification notification);
}
