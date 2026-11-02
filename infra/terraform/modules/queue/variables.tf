variable "name" {
  type = string
}

variable "visibility_timeout_seconds" {
  type    = number
  default = 120
}

variable "max_receive_count" {
  type    = number
  default = 5
}

variable "retention_seconds" {
  type    = number
  default = 345600 # 4 days
}

variable "dlq_retention_seconds" {
  description = "Long on purpose: parked messages must survive a weekend until someone redrives them."
  type        = number
  default     = 1209600 # 14 days (SQS maximum)
}

variable "allowed_sources" {
  description = "AWS services allowed to send to this queue (S3 notifications, SNS subscriptions), scoped by source ARN."
  type = list(object({
    sid        = string
    service    = string
    source_arn = string
  }))
  default = []
}
