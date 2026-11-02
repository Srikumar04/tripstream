output "arn" {
  value = aws_sqs_queue.main.arn
}

output "name" {
  value = aws_sqs_queue.main.name
}

output "url" {
  value = aws_sqs_queue.main.id
}

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "dlq_name" {
  value = aws_sqs_queue.dlq.name
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}
