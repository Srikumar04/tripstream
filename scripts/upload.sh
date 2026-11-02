#!/usr/bin/env bash
# Usage: scripts/upload.sh <file.csv> [api-base-url]
# Creates a job, PUTs the file to the presigned URL, then polls the job status.
set -euo pipefail
FILE="${1:?usage: upload.sh <file.csv> [api-base-url]}"
API="${2:-http://localhost:8080}"
RESP=$(curl -sf -X POST "$API/uploads")
JOB=$(echo "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin)["jobId"])')
URL=$(echo "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin)["uploadUrl"])')
echo "jobId=$JOB"
curl -sf -X PUT --upload-file "$FILE" "$URL"
for _ in $(seq 1 3); do
  curl -sf "$API/jobs/$JOB"; echo
  sleep 1
done
