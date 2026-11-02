data "aws_caller_identity" "current" {}

locals {
  name = "${var.name_prefix}-${var.environment}"
  # S3 bucket names are global: the account id keeps them unique without a random suffix.
  bucket_prefix = "${local.name}-${data.aws_caller_identity.current.account_id}"
}

# ---- storage ---------------------------------------------------------------------------------------------------

module "buckets" {
  source           = "./modules/buckets"
  name_prefix      = local.bucket_prefix
  force_destroy    = var.force_destroy_buckets
  enable_lifecycle = var.enable_s3_lifecycle
}

module "tables" {
  source      = "./modules/tables"
  name_prefix = local.name
}

# ---- queues (each with its own DLQ) -------------------------------------------------------------------------------

module "file_events" {
  source                     = "./modules/queue"
  name                       = "${local.name}-file-events"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  max_receive_count          = var.max_receive_count
  allowed_sources = [{
    sid        = "AllowS3RawBucket"
    service    = "s3.amazonaws.com"
    source_arn = module.buckets.arns["raw"]
  }]
}

module "chunk_jobs" {
  source                     = "./modules/queue"
  name                       = "${local.name}-chunk-jobs"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  max_receive_count          = var.max_receive_count
}

module "aggregator" {
  source                     = "./modules/queue"
  name                       = "${local.name}-aggregator-queue"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  max_receive_count          = var.max_receive_count
  allowed_sources = [{
    sid        = "AllowTripEventsTopic"
    service    = "sns.amazonaws.com"
    source_arn = module.trip_events.arn
  }]
}

module "notifier" {
  source                     = "./modules/queue"
  name                       = "${local.name}-notifier-queue"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  max_receive_count          = var.max_receive_count
  allowed_sources = [{
    sid        = "AllowTripEventsTopic"
    service    = "sns.amazonaws.com"
    source_arn = module.trip_events.arn
  }]
}

# ---- S3 ObjectCreated (uploads/) -> file-events ---------------------------------------------------------------------

resource "aws_s3_bucket_notification" "raw_uploads" {
  bucket = module.buckets.names["raw"]

  queue {
    queue_arn     = module.file_events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "uploads/"
  }

  depends_on = [module.file_events]
}

# ---- SNS fan-out ----------------------------------------------------------------------------------------------------

module "trip_events" {
  source = "./modules/topic"
  name   = "${local.name}-trip-events"
  subscriptions = {
    aggregator = module.aggregator.arn
    notifier   = module.notifier.arn
  }
}

module "notifications" {
  source = "./modules/topic" # target of the notifier's SNS adapter; subscribe e-mail/SMS/chat here
  name   = "${local.name}-notifications"
}

# ---- alarms ---------------------------------------------------------------------------------------------------------

module "alarms" {
  source                               = "./modules/alarms"
  name                                 = local.name
  alarm_email                          = var.alarm_email
  oldest_message_age_threshold_seconds = var.oldest_message_age_threshold_seconds
  queues = {
    file-events = module.file_events.name
    chunk-jobs  = module.chunk_jobs.name
    aggregator  = module.aggregator.name
    notifier    = module.notifier.name
  }
  dlqs = {
    file-events = module.file_events.dlq_name
    chunk-jobs  = module.chunk_jobs.dlq_name
    aggregator  = module.aggregator.dlq_name
    notifier    = module.notifier.dlq_name
  }
}

# ---- IAM: one least-privilege task role per application role ------------------------------------------------------------

module "iam" {
  source = "./modules/iam"
  name   = local.name

  bucket_arns = module.buckets.arns
  table_arns  = module.tables.arns
  queue_arns = {
    file-events = module.file_events.arn
    chunk-jobs  = module.chunk_jobs.arn
    aggregator  = module.aggregator.arn
    notifier    = module.notifier.arn
  }
  dlq_arns = {
    file-events = module.file_events.dlq_arn
    chunk-jobs  = module.chunk_jobs.dlq_arn
    aggregator  = module.aggregator.dlq_arn
    notifier    = module.notifier.dlq_arn
  }
  trip_events_topic_arn   = module.trip_events.arn
  notifications_topic_arn = module.notifications.arn
}
