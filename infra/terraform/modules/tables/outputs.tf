output "names" {
  value = {
    idempotency  = aws_dynamodb_table.idempotency.name
    job_progress = aws_dynamodb_table.job_progress.name
  }
}

output "arns" {
  value = {
    idempotency  = aws_dynamodb_table.idempotency.arn
    job_progress = aws_dynamodb_table.job_progress.arn
  }
}
