# TripStream

Event-driven trip-processing pipeline on AWS (S3, SQS, SNS, DynamoDB) with Spring Boot 3 / Java 21 and PostgreSQL.
A client uploads a large CSV of synthetic trips; the system validates it, processes it in parallel chunks, stores clean
rows, reports bad rows, and fans out events. Every stage is idempotent and retry-safe.

> Status: Phases 0-4 and 6 done; Phase 5 has the Terraform for storage/messaging/IAM/alarms (verified on LocalStack, not applied to real AWS), ECS/RDS skipped; Phase 7 (polish) next. See the build plan in the project brief.

## Run locally (no AWS account needed)

Prerequisites: Java 21, Maven 3.9+, Docker.

```bash
docker compose up -d          # Postgres 16 + LocalStack (buckets, queues, DLQs, topic, tables)
mvn verify                    # unit + Testcontainers integration tests

# Run one role (pick a profile): api | splitter | worker | aggregator | notifier
mvn spring-boot:run -Dspring-boot.run.profiles=api
```

Local ports: api 8080, splitter 8081, worker 8082, aggregator 8083, notifier 8084. If a host port clashes:
`POSTGRES_PORT=5433 LOCALSTACK_PORT=4577 docker compose up -d` (and set `DB_URL` / `AWS_ENDPOINT` to match).

## Try ingestion

```bash
# start the api first on a fresh DB (it runs the Liquibase migrations, see docs/decisions.md ADR-005)
mvn spring-boot:run -Dspring-boot.run.profiles=api
mvn spring-boot:run -Dspring-boot.run.profiles=splitter     # second terminal
mvn spring-boot:run -Dspring-boot.run.profiles=worker       # third terminal (run several to scale out)

python3 scripts/generate_trips.py --rows 100000 --bad-pct 5 --dup-pct 2 --out data/generated/trips-100k.csv
scripts/upload.sh data/generated/trips-100k.csv             # POST /uploads, PUT to the presigned URL, poll the job
```

- `POST /uploads` creates a job (`CREATED`) and returns `{jobId, key, uploadUrl, expiresAt}`; PUT the CSV to `uploadUrl`.
- S3 `ObjectCreated` on `uploads/` publishes to `file-events`; the splitter role marks the job `RECEIVED`.
- The splitter streams the file, writes chunk files (`chunks/{jobId}/NNNNNN.csv`, default 5000 rows) and enqueues one `chunk-jobs` message each.
- Workers validate rows, batch-upsert good rows, write bad rows to `tripstream-errors/{jobId}/chunk-NNNNNN.csv`, and bump an atomic counter. When every chunk is done the job becomes `COMPLETED` and `BatchCompleted` is published to SNS.
- `GET /jobs/{id}` returns status and counts (404 unknown, 400 malformed id); `GET /jobs/{id}/errors` returns presigned links to the error reports.
- The generator writes `<out>.manifest.json` with the exact expected good/bad/duplicate counts, used for Phase 2 assertions.

## How exactly-once is achieved on at-least-once queues
- **Claim:** a worker conditionally puts `jobId#chunkId` in DynamoDB with a lease. DONE means skip; a live lease held by someone else means retry later; an expired lease means take over.
- **Count once:** marking the chunk DONE and incrementing `chunksDone` is one DynamoDB transaction, so redelivery cannot double-count.
- **Write once:** trips are upserted on the natural key `trip_id` (no-op updates are skipped), so reprocessing a chunk rewrites nothing.
- **Complete once:** both the splitter and the last worker check `chunksDone == chunksTotal`; finalisation is idempotent and the `BatchCompleted` publish is guarded by a flag.
- Verified by `PipelineIT`: 100k rows, 5% bad, 2% duplicates give exactly 93,000 good / 5,000 bad, and re-delivering every chunk message and the file event changes nothing.

## Fan-out
When a job completes, `BatchCompleted` goes to the `trip-events` SNS topic, which fans out to two queues:
- `aggregator` role: recomputes `daily_vehicle_stats` for the vehicle-days the job touched (idempotent, ADR-007).
- `notifier` role: sends a notification through a `NotificationSender` port: `console` adapter by default,
  `--tripstream.notifier.adapter=sns` for the real SNS adapter (`tripstream-notifications` topic).

## Reliability
Retries use exponential backoff and end in a DLQ after 5 receives; a heartbeat keeps slow chunks from being redelivered;
`scripts/redrive.sh <queue>` moves DLQ messages back after you fix the cause. Every failure mode (kill -9 mid-chunk,
duplicates, poison message, missing object, DB outage) is injected for real in `FailureModesIT` and ends in exact counts.
See [docs/failure-modes.md](docs/failure-modes.md).

## Benchmarks (local, LocalStack, one laptop; see [docs/benchmarks.md](docs/benchmarks.md))
| | |
|---|---|
| 5M-row file, 4 workers | 75 s, **66,692 rows/s**, exact counts |
| 1 -> 4 workers (1M rows) | 33,205 -> 67,901 rows/s (2.05x); stops scaling at ~4 workers: row-lock contention, found with `pg_stat_activity` wait events |
| After fixing the contention | **103,663 rows/s** at 4 workers (+53%); 115,216 rows/s at 8 with batched-insert rewrite |
| 4x `kill -9` workers + 35% duplicated messages (1M rows) | zero loss, zero double counting |
| Poison flood (300k bad rows + garbage files) | contained; concurrent legit job exact |

These are not AWS numbers (emulated services, Postgres in Docker on the same machine); the doc explains what transfers.

## Infrastructure as code
`infra/terraform` provisions the buckets, queues + DLQs, SNS fan-out, DynamoDB tables, least-privilege task roles and
CloudWatch alarms (DLQ not empty, oldest message age). See [infra/terraform/README.md](infra/terraform/README.md).

## Layout
- `src/main/java/com/tripstream/roles` - per-profile role wiring
- `src/main/java/com/tripstream/jobs` - upload and job-status API; `.../ingest` - S3 event consumer
- `scripts/` - synthetic data generator and upload helper
- `src/main/resources/db/changelog` - Liquibase schema
- `infra/localstack/init-aws.sh` - idempotent creation of all local AWS resources
- `docs/decisions.md`, `docs/failure-modes.md` - ADRs and failure analysis

## Architecture
_Diagram and trade-offs added in Phase 7._
