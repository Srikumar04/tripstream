resource "aws_sns_topic" "alerts" {
  name = "${var.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Anything in a DLQ means a message failed every retry and needs a human (inspect, fix, redrive).
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  for_each            = var.dlqs
  alarm_name          = "${var.name}-${each.key}-dlq-not-empty"
  alarm_description   = "Messages are parked in ${each.value}. See docs/failure-modes.md (redrive procedure)."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = each.value }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# A growing oldest-message age means consumers are down, stuck or too slow: it fires before anything reaches a DLQ.
resource "aws_cloudwatch_metric_alarm" "oldest_message_age" {
  for_each            = var.queues
  alarm_name          = "${var.name}-${each.key}-oldest-message-age"
  alarm_description   = "Oldest message in ${each.value} is older than ${var.oldest_message_age_threshold_seconds}s."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = each.value }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = var.oldest_message_age_threshold_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}
