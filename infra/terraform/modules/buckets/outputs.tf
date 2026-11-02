output "names" {
  value = { for k, b in aws_s3_bucket.this : k => b.bucket }
}

output "arns" {
  value = { for k, b in aws_s3_bucket.this : k => b.arn }
}
