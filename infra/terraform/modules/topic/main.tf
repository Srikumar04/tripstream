resource "aws_sns_topic" "this" {
  name = var.name
}

resource "aws_sns_topic_subscription" "queue" {
  for_each             = var.subscriptions
  topic_arn            = aws_sns_topic.this.arn
  protocol             = "sqs"
  endpoint             = each.value
  raw_message_delivery = true # consumers read the event JSON directly, not the SNS envelope
}
