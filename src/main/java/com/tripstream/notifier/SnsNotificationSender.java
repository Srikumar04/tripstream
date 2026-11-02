package com.tripstream.notifier;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;

/**
 * Real adapter (tripstream.notifier.adapter=sns): publishes to an SNS topic, which can fan out to email (SES/SNS
 * email subscriptions), SMS, chat webhooks, etc. without the notifier knowing.
 */
@Component
@Profile("notifier")
@ConditionalOnProperty(name = "tripstream.notifier.adapter", havingValue = "sns")
class SnsNotificationSender implements NotificationSender {

    private final SnsClient sns;
    private final String topicName;
    private volatile String topicArn;

    SnsNotificationSender(SnsClient sns,
            @Value("${tripstream.notifier.topic:tripstream-notifications}") String topicName) {
        this.sns = sns;
        this.topicName = topicName;
    }

    @Override
    public void send(Notification n) {
        String arn = topicArn;
        if (arn == null) {
            arn = sns.createTopic(b -> b.name(topicName)).topicArn(); // idempotent
            topicArn = arn;
        }
        String target = arn;
        sns.publish(b -> b.topicArn(target).subject(n.subject()).message(n.body()));
    }
}
