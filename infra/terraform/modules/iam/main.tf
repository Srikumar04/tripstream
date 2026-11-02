# One task role per application role, each limited to exactly what that role's code calls.
# (Database credentials will come from Secrets Manager once RDS is added; not part of this module yet.)

locals {
  roles = toset(["api", "splitter", "worker", "aggregator", "notifier"])

  consume = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
  "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
  send           = ["sqs:SendMessage", "sqs:GetQueueUrl"]
  claim_table_rw = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"]
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  for_each           = local.roles
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# ---- api: presign uploads/downloads, list error reports, run the DLQ redrive endpoint -----------------------------------
data "aws_iam_policy_document" "api" {
  statement {
    sid       = "PresignUploads"
    actions   = ["s3:PutObject"]
    resources = ["${var.bucket_arns["raw"]}/uploads/*"]
  }
  statement {
    sid       = "ReadErrorReports"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arns["errors"]}/*"]
  }
  statement {
    sid       = "ListErrorReports"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arns["errors"]]
  }
  statement {
    sid       = "RedriveReadDlqs"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueUrl"]
    resources = values(var.dlq_arns)
  }
  statement {
    sid       = "RedriveSendToSources"
    actions   = local.send
    resources = values(var.queue_arns)
  }
}

# ---- splitter: raw in, chunks out, consume file-events, enqueue chunk-jobs, record totals, finalise job ------------------
data "aws_iam_policy_document" "splitter" {
  statement {
    sid       = "ReadUploads"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arns["raw"]}/uploads/*"]
  }
  statement {
    sid       = "WriteChunks"
    actions   = ["s3:PutObject"]
    resources = ["${var.bucket_arns["chunks"]}/chunks/*"]
  }
  statement {
    sid       = "ConsumeFileEvents"
    actions   = local.consume
    resources = [var.queue_arns["file-events"]]
  }
  statement {
    sid       = "EnqueueChunks"
    actions   = local.send
    resources = [var.queue_arns["chunk-jobs"]]
  }
  statement {
    sid       = "JobProgress"
    actions   = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [var.table_arns["job_progress"]]
  }
  statement {
    sid       = "PublishBatchCompleted"
    actions   = ["sns:Publish"]
    resources = [var.trip_events_topic_arn]
  }
}

# ---- worker: chunks in, error reports out, consume chunk-jobs, claims + counter, finalise job -----------------------------
data "aws_iam_policy_document" "worker" {
  statement {
    sid       = "ReadChunks"
    actions   = ["s3:GetObject"]
    resources = ["${var.bucket_arns["chunks"]}/chunks/*"]
  }
  statement {
    sid       = "WriteErrorReports"
    actions   = ["s3:PutObject"]
    resources = ["${var.bucket_arns["errors"]}/*"]
  }
  statement {
    sid       = "ConsumeChunkJobs"
    actions   = local.consume
    resources = [var.queue_arns["chunk-jobs"]]
  }
  statement {
    sid       = "Claims"
    actions   = local.claim_table_rw
    resources = [var.table_arns["idempotency"]]
  }
  statement {
    sid       = "JobProgress"
    actions   = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [var.table_arns["job_progress"]]
  }
  statement {
    sid       = "PublishBatchCompleted"
    actions   = ["sns:Publish"]
    resources = [var.trip_events_topic_arn]
  }
}

# ---- aggregator: queue only (database access is separate) -----------------------------------------------------------------
data "aws_iam_policy_document" "aggregator" {
  statement {
    sid       = "ConsumeAggregatorQueue"
    actions   = local.consume
    resources = [var.queue_arns["aggregator"]]
  }
}

# ---- notifier: queue, dedupe claims, publish notifications ------------------------------------------------------------------
data "aws_iam_policy_document" "notifier" {
  statement {
    sid       = "ConsumeNotifierQueue"
    actions   = local.consume
    resources = [var.queue_arns["notifier"]]
  }
  statement {
    sid       = "DedupeClaims"
    actions   = local.claim_table_rw
    resources = [var.table_arns["idempotency"]]
  }
  statement {
    sid       = "Notify"
    actions   = ["sns:Publish"]
    resources = [var.notifications_topic_arn]
  }
}

locals {
  documents = {
    api        = data.aws_iam_policy_document.api.json
    splitter   = data.aws_iam_policy_document.splitter.json
    worker     = data.aws_iam_policy_document.worker.json
    aggregator = data.aws_iam_policy_document.aggregator.json
    notifier   = data.aws_iam_policy_document.notifier.json
  }
}

resource "aws_iam_role_policy" "this" {
  for_each = local.roles
  name     = "${var.name}-${each.key}"
  role     = aws_iam_role.this[each.key].id
  policy   = local.documents[each.key]
}
