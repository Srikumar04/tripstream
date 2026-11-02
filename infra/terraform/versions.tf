terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Pointing at LocalStack (var.localstack_endpoint) exercises the whole graph without an AWS account.
  # Left null for real AWS, where all of these settings stay at their strict defaults.
  access_key                  = var.localstack_endpoint == null ? null : "test"
  secret_key                  = var.localstack_endpoint == null ? null : "test"
  skip_credentials_validation = var.localstack_endpoint != null
  skip_metadata_api_check     = var.localstack_endpoint != null
  skip_requesting_account_id  = var.localstack_endpoint != null
  s3_use_path_style           = var.localstack_endpoint != null

  endpoints {
    s3         = var.localstack_endpoint
    sqs        = var.localstack_endpoint
    sns        = var.localstack_endpoint
    dynamodb   = var.localstack_endpoint
    iam        = var.localstack_endpoint
    cloudwatch = var.localstack_endpoint
    sts        = var.localstack_endpoint
  }

  default_tags {
    tags = {
      Project     = "tripstream"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
