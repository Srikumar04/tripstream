# Architecture Decision Records

Section-5 tech choices are fixed by the project brief; deviations get an entry here with a written reason.

## ADR-001: Spring Boot 3.4.x with Spring Cloud AWS 3.3.x
- Status: accepted
- Context: Brief mandates Spring Boot 3.x and Spring Cloud AWS 3.x (AWS SDK v2).
- Decision: Boot 3.4.5 + Spring Cloud AWS 3.3.0.
- Consequences: Revisit when upgrading Boot; Spring Cloud AWS minor must stay compatible with the Boot minor.

## ADR-002: Pin LocalStack to 3.8.1
- Status: accepted
- Context: Newer LocalStack images require an auth token even for the free tier, breaking "docker compose up with no account".
- Decision: Pin `localstack/localstack:3.8.1` (community edition).
- Consequences: Some newer LocalStack features are unavailable; revisit if the licensing model changes.

## ADR-003: One codebase, five roles via Spring profiles
- Status: accepted
- Decision: `api`, `splitter`, `worker`, `aggregator`, `notifier` profiles; each exposes actuator health on its own local port (8080-8084) so all roles can run side by side.
- Consequences: Roles scale independently in ECS; shared code lives in one module (no multi-module overhead).

## ADR-004: Resource creation in a single idempotent shell script
- Status: accepted
- Decision: `infra/localstack/init-aws.sh` runs in LocalStack's `ready.d` hook and is also exercised by an integration test, so local setup cannot silently drift. Terraform (Phase 5) becomes the source of truth for AWS.

## ADR-005: Only the `api` role runs Liquibase migrations
- Status: accepted
- Context: Found in a live smoke test: starting `api` and `splitter` simultaneously against an empty database made one crash with a duplicate-key error while creating `databasechangeloglock`. Liquibase's lock protects migrations, but not the creation of the lock table itself.
- Decision: `spring.liquibase.enabled=false` by default, `true` under the `api` profile. Start `api` first on a fresh database.
- Consequences: Other roles that start before the schema exists fail their first queries and rely on SQS redelivery. In Phase 5 this becomes a one-shot migration task run before the services (ECS task / init container), at which point the api flag can be turned off too.

## ADR-006: Chunk upserts run in trip_id order
- Status: accepted
- Context: Found by the 100k-row pipeline test. Duplicate trips can land in different chunks, so parallel workers upsert overlapping key sets. With each worker taking row locks in file order, Postgres reported `deadlock detected`, the chunks were retried only after the visibility timeout, and the job stalled.
- Decision: sort each chunk's rows by `trip_id` before the batch upsert, so all transactions acquire row locks in the same global order.
- Consequences: no lock cycles between chunks; deadlocks remain a retryable error (SQS redelivery) as a safety net.

## ADR-007: The aggregator recomputes affected (vehicle, day) rows instead of adding deltas
- Status: accepted
- Context: `daily_vehicle_stats` is fed by at-least-once BatchCompleted events, and a later upload can re-send trips that already exist (same `trip_id`, upserted). Incrementing counters per event double-counts on either.
- Decision: for each (vehicle, UTC day) touched by the job, recompute count and distance from the `trips` table and overwrite the row (`INSERT ... SELECT ... ON CONFLICT DO UPDATE SET = EXCLUDED`). An event-id row in `processed_events` (same transaction) skips duplicate work, and a transaction-scoped advisory lock serialises recomputes so concurrent events cannot overwrite each other with a stale snapshot.
- Consequences: idempotent and order-insensitive without bookkeeping per delta; the cost is a scan of the touched vehicle-days per event (supported by an expression index) and a single-writer lock (fine: one statement per job). If a re-upload moves a trip to another vehicle or day, the *old* vehicle-day is not recomputed until another job touches it.

## ADR-008: Fan-out over SNS with raw message delivery; notifier behind a port
- Status: accepted
- Decision: `trip-events` fans out to `aggregator-queue` and `notifier-queue` (SQS subscriptions with RawMessageDelivery, so consumers read the event JSON directly). Each consumer has its own queue and DLQ, so one slow or failing consumer never blocks the other. The notifier depends on a `NotificationSender` interface with a console adapter (default) and an SNS adapter (which can reach email/SMS/chat).
- Consequences: event ids are stable per job (`BatchCompleted:<jobId>`); consumers dedupe on them (aggregator via Postgres, notifier via the DynamoDB claim pattern). Notification delivery is at-least-once. On real AWS the SQS queue policies must allow `sns.amazonaws.com` (Terraform, Phase 5); LocalStack does not enforce it.

## ADR-009: Terraform layout, least-privilege roles and what was left out
- Status: accepted
- Decision: one small module per concern (`buckets`, `queue` = queue + DLQ, `topic`, `tables`, `alarms`, `iam`) composed by a root that also wires the S3 notification. Resource names carry `<prefix>-<environment>`; bucket names add the account id for global uniqueness. Roles are per application role with resource-scoped statements (for example the splitter can read `raw/uploads/*` and write `chunks/chunks/*`, nothing else). The app takes topic ARNs from configuration (`TRIPSTREAM_TOPICS_TRIP_EVENTS_ARN`, `TRIPSTREAM_NOTIFIER_TOPIC_ARN`) so no role needs `sns:CreateTopic`.
- Left out on purpose: `aws_sqs_queue_redrive_allow_policy` ("only the main queue may use this DLQ"). It is optional hardening, and its destroy never finished against LocalStack, so I could not verify clean teardown. The queue's redrive policy (which is what sends messages to the DLQ) is unaffected.
- Alarms: DLQ depth > 0 is the "a human must act" signal; oldest-message-age is the "consumers are down or slow" signal and fires earlier. Neither needs the application to emit metrics.
- Not applied to real AWS yet. ECS/RDS are deferred (see infra/terraform/README.md).

## ADR-010: Findings from load testing: pool sizing, timestamps, and an opt-in commit-per-batch mode
- Status: accepted (commit-per-batch stays **off by default**)
- Context: Phase 6 benchmarks (docs/benchmarks.md). Scaling stopped at about 4 workers because concurrent chunk transactions waited on each other's row locks (74% of active DB sessions on `Lock:transactionid` at 8 workers), not because of CPU.
- Decisions:
  1. `tripstream.worker.commit-per-batch` (default `false`). When `true`, each 500-row JDBC batch commits on its own, so locks are held for milliseconds. Measured +53% at 4 workers (67.9k to 103.7k rows/s). Safe because upserts are idempotent and the chunk is marked done only after its last batch; the cost is that a running chunk's rows can be partially visible in `trips`. Left off by default because the failure-mode suite (`FailureModesIT`) was written and run against the single-transaction behaviour; it was validated at 1M rows with kill -9 chaos instead (docs/benchmarks.md section 4).
  2. `DB_POOL_SIZE` (default 10) makes the Hikari pool size configurable: instances x pool size must stay under the database's `max_connections` (the 8-worker benchmark hit `too many clients`). A worker needs about `concurrency + 2`.
  3. Chunk timestamps use `clock_timestamp()`; `now()` inside the DB transaction returned the transaction start and hid the upsert time.
- Not done: enabling `reWriteBatchedInserts` by default (+21% on its own) pending a look at its interaction with `ON CONFLICT` on other pgjdbc versions.
