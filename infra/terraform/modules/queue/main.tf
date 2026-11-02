resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = var.dlq_retention_seconds
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "main" {
  name                       = var.name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.retention_seconds
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

data "aws_iam_policy_document" "send" {
  count = length(var.allowed_sources) > 0 ? 1 : 0

  dynamic "statement" {
    for_each = var.allowed_sources
    content {
      sid       = statement.value.sid
      effect    = "Allow"
      actions   = ["sqs:SendMessage"]
      resources = [aws_sqs_queue.main.arn]

      principals {
        type        = "Service"
        identifiers = [statement.value.service]
      }
      condition {
        test     = "ArnEquals"
        variable = "aws:SourceArn"
        values   = [statement.value.source_arn]
      }
    }
  }
}

resource "aws_sqs_queue_policy" "send" {
  count     = length(var.allowed_sources) > 0 ? 1 : 0
  queue_url = aws_sqs_queue.main.id
  policy    = data.aws_iam_policy_document.send[0].json
}
