# Idempotency claims and notification dedupe. Items expire through the "ttl" attribute (set to now + 7 days).
resource "aws_dynamodb_table" "idempotency" {
  name         = "${var.name_prefix}-idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# Atomic chunk counter per job (chunksTotal / chunksDone). Small and short-lived, no TTL needed for the demo.
resource "aws_dynamodb_table" "job_progress" {
  name         = "${var.name_prefix}-job-progress"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "jobId"

  attribute {
    name = "jobId"
    type = "S"
  }
}
