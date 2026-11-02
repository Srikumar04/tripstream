#!/usr/bin/env bash
# Usage: scripts/redrive.sh <queue> [api-base-url] [max]
# Moves messages from <queue>-dlq back to <queue> via the api role's admin endpoint.
# Queues: file-events | chunk-jobs | aggregator-queue | notifier-queue
set -euo pipefail
QUEUE="${1:?usage: redrive.sh <queue> [api-base-url] [max]}"
API="${2:-http://localhost:8080}"
curl -sf -X POST "$API/admin/redrive/$QUEUE?max=${3:-1000}"; echo
