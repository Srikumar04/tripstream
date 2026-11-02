package com.tripstream.notifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Local/mock adapter: writes the notification to the log. Default (tripstream.notifier.adapter=console). */
@Component
@Profile("notifier")
@ConditionalOnProperty(name = "tripstream.notifier.adapter", havingValue = "console", matchIfMissing = true)
class ConsoleNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger("notifications");

    @Override
    public void send(Notification n) {
        log.info("NOTIFICATION [{}] {} :: {}", n.level(), n.subject(), n.body());
    }
}
