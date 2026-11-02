locals {
  buckets = toset(["raw", "chunks", "errors"])
}

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = "${var.name_prefix}-${each.key}"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = var.enable_lifecycle ? local.buckets : toset([])
  bucket   = aws_s3_bucket.this[each.key].id

  rule {
    id     = "expire"
    status = "Enabled"
    filter {
      prefix = ""
    }

    expiration {
      days = var.expiration_days[each.key]
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
