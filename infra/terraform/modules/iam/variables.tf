variable "name" {
  type = string
}

variable "bucket_arns" {
  description = "keys: raw, chunks, errors"
  type        = map(string)
}

variable "table_arns" {
  description = "keys: idempotency, job_progress"
  type        = map(string)
}

variable "queue_arns" {
  description = "keys: file-events, chunk-jobs, aggregator, notifier"
  type        = map(string)
}

variable "dlq_arns" {
  description = "same keys as queue_arns"
  type        = map(string)
}

variable "trip_events_topic_arn" {
  type = string
}

variable "notifications_topic_arn" {
  type = string
}
