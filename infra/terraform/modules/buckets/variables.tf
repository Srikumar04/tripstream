variable "name_prefix" {
  type = string
}

variable "force_destroy" {
  type    = bool
  default = true
}

variable "expiration_days" {
  description = "Object lifetime per bucket. Chunks are scratch data; raw uploads and error reports are kept longer."
  type        = map(number)
  default = {
    raw    = 30
    chunks = 7
    errors = 30
  }
}

variable "enable_lifecycle" {
  description = "Expire old objects. Disable only when applying against LocalStack (see root variable)."
  type        = bool
  default     = true
}
