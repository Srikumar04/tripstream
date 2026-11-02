#!/usr/bin/env bash
# TripStream demo, about 2 minutes. Designed to be screen-recorded.
#   Act 1  happy path: upload 100k rows, watch the job complete, error report, stats, notification
#   Act 2  failure path: upload 1M rows, kill -9 a worker mid-run, still exact counts, nothing double counted
# Usage: scripts/demo.sh [--keep]        (--keep leaves the stack running afterwards)
# Env:   POSTGRES_PORT / LOCALSTACK_PORT if the defaults clash, DEMO_PAUSE=seconds between steps (default 2),
#        DEMO_API_PORT (default 18080)
# Needs: docker, java 21, python3, and a built jar (built automatically with mvn if missing).
set -euo pipefail
cd "$(dirname "$0")/.."

PAUSE="${DEMO_PAUSE:-2}"
API_PORT="${DEMO_API_PORT:-18080}"
API="http://localhost:${API_PORT}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}" LOCALSTACK_PORT="${LOCALSTACK_PORT:-4566}"
export DB_URL="jdbc:postgresql://localhost:${POSTGRES_PORT}/tripstream"
export AWS_ENDPOINT="http://localhost:${LOCALSTACK_PORT}"
export DB_POOL_SIZE=6
JAR=target/tripstream-0.1.0-SNAPSHOT.jar
LOGS=data/generated/demo-logs
DATA=data/generated
KEEP=false; [[ "${1:-}" == "--keep" ]] && KEEP=true
PIDS=()
mkdir -p "$LOGS" "$DATA"

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; D=$'\033[2m'; N=$'\033[0m'
say()  { printf "\n%s== %s ==%s\n" "$B" "$*" "$N"; sleep "$PAUSE"; }
note() { printf "%s%s%s\n" "$D" "$*" "$N"; }
ok()   { printf "%s%s%s\n" "$G" "$*" "$N"; }
psql_() { docker compose exec -T postgres psql -U tripstream -d tripstream -At -c "$1"; }
awslocal_() { docker compose exec -T localstack bash -c "$1"; }
jget() { python3 -c "import json,sys; d=json.load(sys.stdin); print(d$1)"; }

cleanup() {
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null || true; done
  if ! $KEEP; then docker compose down -v >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

start_role() { # name profile [args...]
  local name=$1 profile=$2; shift 2
  java -Xmx384m -jar "$JAR" --spring.profiles.active="$profile" "$@" >"$LOGS/$name.log" 2>&1 &
  PIDS+=($!); LAST_PID=$!
  disown "$LAST_PID"   # no "Killed: 9" job-control noise on screen when we kill a worker or clean up
  for _ in $(seq 1 120); do
    grep -q "Started TripStreamApplication" "$LOGS/$name.log" 2>/dev/null && return 0
    kill -0 "$LAST_PID" 2>/dev/null || { echo "$name failed to start, see $LOGS/$name.log"; exit 1; }
    sleep 1
  done
  echo "$name did not start in time"; exit 1
}

upload() { # file -> prints jobId
  local resp url
  resp=$(curl -sf -X POST "$API/uploads")
  url=$(echo "$resp" | jget "['uploadUrl']")
  curl -sf -X PUT --upload-file "$1" "$url"
  echo "$resp" | jget "['jobId']"
}

watch_job() { # jobId, optional callback evaluated each poll
  local id=$1 last="" st line
  while true; do
    line=$(curl -sf "$API/jobs/$id")
    st=$(echo "$line" | jget "['status']")
    done_n=$(psql_ "SELECT count(*) FROM job_chunks WHERE job_id='$id' AND status='DONE'")
    total_n=$(psql_ "SELECT count(*) FROM job_chunks WHERE job_id='$id'")
    cur="$st  chunks done ${done_n}/${total_n}"
    [[ "$cur" != "$last" ]] && { printf "  %s\n" "$cur"; last="$cur"; }
    [[ -n "${2:-}" ]] && $2 "$id" "$done_n" "$total_n"
    [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break
    sleep 1
  done
}

say "Starting the stack: Postgres + LocalStack (S3, SQS, SNS, DynamoDB), then the five roles as separate JVMs"
[[ -f "$JAR" ]] || { note "building the jar..."; mvn -q -DskipTests -DskipITs package; }
docker compose up -d --wait 2>&1 | tail -2
start_role api api --server.port="$API_PORT"; ok "api up"
# short lease/visibility so Act 2 recovers in seconds, not minutes (all configurable)
TIMING=(--tripstream.worker.lease=PT10S --tripstream.reliability.heartbeat-interval=PT3S
        --tripstream.reliability.visibility-timeout=PT15S)
start_role splitter splitter --server.port=0 "${TIMING[@]}"; ok "splitter up"
for i in 1 2 3; do start_role "worker-$i" worker --server.port=0 --tripstream.worker.concurrency=1 \
  --tripstream.worker.poll-size=1 "${TIMING[@]}"; WORKER_PIDS[$i]=$LAST_PID; done; ok "3 workers up"
start_role aggregator aggregator --server.port=0; ok "aggregator up"
start_role notifier notifier --server.port=0; ok "notifier up"
psql_ "TRUNCATE trips, job_chunks, jobs, daily_vehicle_stats, processed_events" >/dev/null
Q="http://localhost:4566/000000000000"
awslocal_ "for q in file-events chunk-jobs aggregator-queue notifier-queue file-events-dlq chunk-jobs-dlq; do awslocal sqs purge-queue --queue-url $Q/\$q >/dev/null 2>&1; done; for q in file-events chunk-jobs; do awslocal sqs set-queue-attributes --queue-url $Q/\$q --attributes VisibilityTimeout=15; done"

# ----------------------------------------------------------------------------------------------- Act 1
say "ACT 1  Happy path: 100,000 synthetic trips, 5% bad rows, 2% duplicates"
python3 scripts/generate_trips.py --rows 100000 --bad-pct 5 --dup-pct 2 --out "$DATA/demo-100k.csv"
note "expected (from the generator's manifest): $(tr -d '\n ' < "$DATA/demo-100k.csv.manifest.json")"
sleep "$PAUSE"
JOB=$(upload "$DATA/demo-100k.csv"); echo "uploaded, jobId=$JOB"
watch_job "$JOB"
echo; curl -sf "$API/jobs/$JOB" | python3 -m json.tool
GOOD=$(curl -sf "$API/jobs/$JOB" | jget "['goodRows']"); BAD=$(curl -sf "$API/jobs/$JOB" | jget "['badRows']")
[[ "$GOOD" == "93000" && "$BAD" == "5000" ]] && ok "exact: 93,000 good + 5,000 bad + 2,000 duplicates collapsed" || { echo "${R}MISMATCH good=$GOOD bad=$BAD$N"; exit 1; }
sleep "$PAUSE"
note "error report (bad rows with the reason):"
ERR_URL=$(curl -sf "$API/jobs/$JOB/errors" | jget "['files'][0]['url']")
curl -sf "$ERR_URL" | head -4
sleep "$PAUSE"
note "aggregator output, the busiest vehicle-days (BatchCompleted -> SNS -> aggregator-queue):"
for _ in $(seq 1 30); do [[ "$(psql_ 'SELECT count(*) FROM daily_vehicle_stats')" -gt 0 ]] && break; sleep 1; done
psql_ "SELECT vehicle_id, day, trip_count, total_km FROM daily_vehicle_stats ORDER BY trip_count DESC, vehicle_id LIMIT 3"
for _ in $(seq 1 20); do grep -q NOTIFICATION "$LOGS/notifier.log" && break; sleep 1; done
note "notifier (console adapter):"; grep NOTIFICATION "$LOGS/notifier.log" | tail -1 | sed -E 's/^.* NOTIFICATION/NOTIFICATION/'

# ----------------------------------------------------------------------------------------------- Act 2
say "ACT 2  Failure path: 1,000,000 trips, then kill -9 a worker in the middle of the run"
[[ -f "$DATA/trips-1000000.csv" ]] || python3 scripts/generate_trips.py --rows 1000000 --bad-pct 5 --dup-pct 2 --out "$DATA/trips-1000000.csv"
note "expected: $(tr -d '\n ' < "$DATA/trips-1000000.csv.manifest.json")"
JOB2=$(upload "$DATA/trips-1000000.csv"); echo "uploaded, jobId=$JOB2"
KILLED=false
maybe_kill() { # jobId done total
  if ! $KILLED && [[ "$2" -ge 30 ]]; then
    KILLED=true
    printf "%s  >>> kill -9 worker-2 (pid %s) with chunks in flight <<<%s\n" "$R" "${WORKER_PIDS[2]}" "$N"
    kill -9 "${WORKER_PIDS[2]}"
  fi
}
watch_job "$JOB2" maybe_kill
echo; curl -sf "$API/jobs/$JOB2" | python3 -m json.tool
note "the killed worker's claim expired and another worker redid its chunk (attempts > 1 on those chunks):"
psql_ "SELECT chunk_id, attempts FROM job_chunks WHERE job_id='$JOB2' AND attempts > 1 ORDER BY chunk_id LIMIT 5"
CHUNKS=$(psql_ "SELECT count(*) FROM job_chunks WHERE job_id='$JOB2'")
COUNTED=$(awslocal_ "awslocal dynamodb get-item --table-name tripstream-job-progress --key '{\"jobId\":{\"S\":\"$JOB2\"}}' --query 'Item.chunksDone.N' --output text" | tr -d '\r')
TRIPS=$(psql_ "SELECT count(*) FROM trips WHERE job_id='$JOB2'")
GOOD2=$(curl -sf "$API/jobs/$JOB2" | jget "['goodRows']")
echo "chunks: $CHUNKS, counted exactly once in DynamoDB: $COUNTED, distinct trips stored: $TRIPS"
[[ "$COUNTED" == "$CHUNKS" && "$GOOD2" == "930000" && "$TRIPS" == "930000" ]] \
  && ok "zero loss, zero double counting despite the crash" || { echo "${R}MISMATCH$N"; exit 1; }
say "Done. Every number above came from the running system; compare with docs/benchmarks.md and docs/failure-modes.md"
