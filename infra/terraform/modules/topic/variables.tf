variable "name" {
  type = string
}

variable "subscriptions" {
  description = "Queue subscribers, name => queue ARN. The queue must allow this topic to send (queue module allowed_sources)."
  type        = map(string)
  default     = {}
}
