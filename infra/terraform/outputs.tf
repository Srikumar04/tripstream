output "app_environment" {
  description = "Environment variables for the TripStream roles (Spring relaxed binding). Feed these to the task definitions."
  value = {
    TRIPSTREAM_BUCKETS_RAW                    = module.buckets.names["raw"]
    TRIPSTREAM_BUCKETS_CHUNKS                 = module.buckets.names["chunks"]
    TRIPSTREAM_BUCKETS_ERRORS                 = module.buckets.names["errors"]
    TRIPSTREAM_QUEUES_FILE_EVENTS             = module.file_events.name
    TRIPSTREAM_QUEUES_CHUNK_JOBS              = module.chunk_jobs.name
    TRIPSTREAM_QUEUES_AGGREGATOR              = module.aggregator.name
    TRIPSTREAM_QUEUES_NOTIFIER                = module.notifier.name
    TRIPSTREAM_TABLES_IDEMPOTENCY             = module.tables.names["idempotency"]
    TRIPSTREAM_TABLES_JOB_PROGRESS            = module.tables.names["job_progress"]
    TRIPSTREAM_TOPICS_TRIP_EVENTS_ARN         = module.trip_events.arn
    TRIPSTREAM_NOTIFIER_TOPIC_ARN             = module.notifications.arn
    TRIPSTREAM_RELIABILITY_VISIBILITY_TIMEOUT = "PT${var.visibility_timeout_seconds}S"
  }
}

output "role_arns" {
  description = "Task role ARN per application role."
  value       = module.iam.role_arns
}

output "alerts_topic_arn" {
  value = module.alarms.alerts_topic_arn
}

output "dlq_urls" {
  value = {
    file-events = module.file_events.dlq_url
    chunk-jobs  = module.chunk_jobs.dlq_url
    aggregator  = module.aggregator.dlq_url
    notifier    = module.notifier.dlq_url
  }
}
