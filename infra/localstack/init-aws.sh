#!/bin/bash
# Idempotent: safe to run repeatedly. Creates all local AWS resources for TripStream.
# Runs inside LocalStack (ready.d hook), where `awslocal` is available.
set -euo pipefail

MAX_RECEIVE_COUNT="${MAX_RECEIVE_COUNT:-5}"
VISIBILITY_TIMEOUT="${VISIBILITY_TIMEOUT:-120}"

echo "[init] creating buckets"
for b in tripstream-raw tripstream-chunks tripstream-errors; do
  awslocal s3api head-bucket --bucket "$b" 2>/dev/null || awslocal s3api create-bucket --bucket "$b" >/dev/null
done

# create_queue_with_dlq <name>: main queue redrives to <name>-dlq after MAX_RECEIVE_COUNT receives
create_queue_with_dlq() {
  local name="$1"
  awslocal sqs create-queue --queue-name "${name}-dlq" >/dev/null
  local dlq_url dlq_arn
  dlq_url=$(awslocal sqs get-queue-url --queue-name "${name}-dlq" --query QueueUrl --output text)
  dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" --attribute-names QueueArn \
            --query Attributes.QueueArn --output text)
  local redrive="{\\\"deadLetterTargetArn\\\":\\\"${dlq_arn}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE_COUNT}\\\"}"
  awslocal sqs create-queue --queue-name "$name" \
    --attributes "{\"VisibilityTimeout\":\"${VISIBILITY_TIMEOUT}\",\"RedrivePolicy\":\"${redrive}\"}" >/dev/null
  echo "[init] queue ${name} (+dlq, maxReceiveCount=${MAX_RECEIVE_COUNT})"
}

create_queue_with_dlq file-events
create_queue_with_dlq chunk-jobs
# Fan-out consumers (subscribed to the SNS topic in Phase 4)
create_queue_with_dlq aggregator-queue
create_queue_with_dlq notifier-queue

echo "[init] creating SNS topic"
awslocal sns create-topic --name trip-events >/dev/null

echo "[init] creating DynamoDB tables"
create_table() {
  local table="$1" key="$2"
  awslocal dynamodb describe-table --table-name "$table" >/dev/null 2>&1 && return 0
  awslocal dynamodb create-table --table-name "$table" \
    --attribute-definitions "AttributeName=${key},AttributeType=S" \
    --key-schema "AttributeName=${key},KeyType=HASH" \
    --billing-mode PAY_PER_REQUEST >/dev/null
}
create_table tripstream-idempotency pk
awslocal dynamodb update-time-to-live --table-name tripstream-idempotency \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null
create_table tripstream-job-progress jobId

echo "[init] TripStream local infra ready"
