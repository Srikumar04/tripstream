variable "name" {
  type = string
}

variable "queues" {
  description = "key => main queue name"
  type        = map(string)
}

variable "dlqs" {
  description = "key => DLQ name"
  type        = map(string)
}

variable "oldest_message_age_threshold_seconds" {
  type    = number
  default = 300
}

variable "alarm_email" {
  type    = string
  default = ""
}
