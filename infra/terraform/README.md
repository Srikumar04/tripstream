# TripStream infrastructure (Terraform)

Creates everything TripStream needs on AWS except the compute and the relational database (see "Not included yet").

| Module | Creates |
|---|---|
| `buckets` | `raw`, `chunks`, `errors` S3 buckets: public access blocked, SSE-S3, lifecycle expiry (30d / 7d / 30d) |
| `queue` (x4) | `file-events`, `chunk-jobs`, `aggregator-queue`, `notifier-queue`, each with a DLQ (14-day retention), redrive policy (max receive count 5), SQS-managed SSE, and a send policy scoped to the S3 bucket / SNS topic that feeds it |
| `topic` (x2) | `trip-events` (raw-delivery SQS subscriptions for the aggregator and notifier) and `notifications` (target of the notifier's SNS adapter) |
| `tables` | DynamoDB `idempotency` (TTL on `ttl`) and `job-progress`, on-demand billing |
| `alarms` | per DLQ: visible messages > 0; per main queue: oldest message age > 300s; all notify an alerts topic (optional e-mail) |
| `iam` | one task role per app role (`api`, `splitter`, `worker`, `aggregator`, `notifier`), each limited to the exact S3/SQS/DynamoDB/SNS actions that role's code calls |

The root also wires S3 `ObjectCreated` on `uploads/` to `file-events`. The `app_environment` output is the exact set of
environment variables the application roles need (Spring relaxed binding), ready to drop into task definitions.

## Use
```bash
cd infra/terraform
terraform init
terraform apply -var environment=dev [-var alarm_email=you@example.com]   # review the plan first
terraform output app_environment
terraform destroy -var environment=dev                                     # buckets are force-destroyed by default
```
Cost while idle is close to zero (on-demand DynamoDB, standard SQS/SNS, a few alarms at ~$0.10 each per month).
`force_destroy_buckets = true` makes teardown cheap and complete; set it to `false` for anything you want to keep.

## Verified without an AWS account
`terraform validate` passes, and the whole stack was applied to a LocalStack container (IAM, CloudWatch, S3, SQS, SNS,
DynamoDB): 46 resources created, the real pipeline then ran against them using `app_environment` (file upload ->
S3 notification -> splitter -> workers -> `BatchCompleted` -> aggregator + notifier, exact counts), and
`terraform destroy` removed everything in about 35 seconds. To repeat it:
```bash
docker run -d --name tf-ls -p 4566:4566 -e SERVICES=s3,sqs,sns,dynamodb,iam,cloudwatch,sts localstack/localstack:3.8.1
terraform apply -var localstack_endpoint=http://localhost:4566 -var enable_s3_lifecycle=false
```
`enable_s3_lifecycle=false` is needed on LocalStack only: AWS provider 5.100 waits for a lifecycle response field
LocalStack 3.8 does not return and times out. Keep it `true` (the default) on AWS. This has **not** been applied to real AWS.

## Not included yet
- **Compute (ECS Fargate) and the database (RDS PostgreSQL).** The task roles are created and ready to be attached; the
  database credentials will come from Secrets Manager (the IAM module will gain a read permission for the roles that use
  the database). Until then `docker compose` plus the app jars is the supported way to run the roles.
- **Redrive endpoint authentication** (documented gap in `docs/failure-modes.md`).
