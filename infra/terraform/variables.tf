variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  description = "Short environment name, part of every resource name."
  type        = string
  default     = "dev"
}

variable "name_prefix" {
  type    = string
  default = "tripstream"
}

variable "visibility_timeout_seconds" {
  description = "Queue visibility timeout. Keep equal to tripstream.reliability.visibility-timeout and above worker.lease."
  type        = number
  default     = 120
}

variable "max_receive_count" {
  description = "Receives before a message moves to the DLQ."
  type        = number
  default     = 5
}

variable "oldest_message_age_threshold_seconds" {
  description = "Alarm when the oldest message in a main queue is older than this."
  type        = number
  default     = 300
}

variable "alarm_email" {
  description = "Optional e-mail address subscribed to the alerts topic (needs confirming). Empty = no subscription."
  type        = string
  default     = ""
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to delete non-empty buckets (cheap teardown for a demo environment)."
  type        = bool
  default     = true
}

variable "localstack_endpoint" {
  description = "Set to e.g. http://localhost:4566 to apply against LocalStack instead of AWS."
  type        = string
  default     = null
}

variable "enable_s3_lifecycle" {
  description = "Keep true on AWS. Set false only against LocalStack 3.8: provider 5.100 waits for a lifecycle response field LocalStack does not return and times out."
  type        = bool
  default     = true
}
