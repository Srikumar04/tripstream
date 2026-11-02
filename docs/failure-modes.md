# Failure Modes

For each failure: what happens, why data is still correct, and which test proves it (`FailureModesIT` unless noted).
The invariant every test ends on: **exact good/bad counts, every chunk counted exactly once.**

## Mechanisms in one place
| Mechanism | Where | Purpose |
|---|---|---|
| Visibility timeout (120s) + max receive count 5 | `infra/localstack/init-aws.sh` (Terraform in Phase 5) | unacked message reappears; after 5 receives it moves to the DLQ |
| Exponential backoff + jitter | `RetryBackoffErrorHandler` | failed message reappears after `base * 2^(receives-1)` (2s base, 5m cap), not immediately |
| Heartbeat | `Heartbeat`, `ChunkProcessor`, `Splitter` | while working, extend the message's visibility and the chunk's claim lease every 30s |
| Claim with lease | `ChunkLedger.claim` (DynamoDB conditional put) | one live owner per chunk; a dead owner's claim expires |
| Atomic "done + count" | `ChunkLedger.completeChunk` (DynamoDB transaction) | `chunksDone` increments at most once per chunk |
| Natural-key upsert, sorted | `TripWriter`, `ChunkProcessor` | reprocessing rewrites nothing; sorted order prevents deadlocks (ADR-006) |
| Idempotent completion | `JobCompletion` | whoever sees `chunksDone == chunksTotal` finalises; repeating it is harmless |

Timing rule: **lease (90s) < visibility timeout (120s)**, and the heartbeat interval (30s) is well under both. A dead
worker's lease expires before its message reappears, so the redelivered message can always take over.

## Worker killed mid-chunk (`workerKilledMidChunkIsRecoveredByAnotherWorker`)
- **Happens:** the process vanishes (the test uses a real `kill -9` on a separate worker JVM): no ack, no claim release, heartbeats stop.
- **Recovery:** the claim's lease expires, the message's visibility expires, SQS redelivers, another worker claims the chunk and processes it.
- **Why correct:** nothing was committed for the chunk (DB writes are one transaction; the counter moves only in the final DynamoDB transaction). Redoing it is safe because upserts are idempotent and the counter is incremented once.
- **Crash after the DB commit but before the counter:** the retry rewrites identical rows (no-op) then increments once.
- **Crash after the counter but before the ack:** the redelivery sees DONE, skips the work and re-runs the completion check.

## Duplicate delivery
- **After completion** (`PipelineIT`): every chunk message and the file event redelivered; DB rows, counters, attempts and event count are unchanged. DONE claims make it a no-op.
- **While the chunk is in flight** (`duplicateMessagesDeliveredWhileTheChunkIsInFlightAreNotDoubleCounted`): the copy hits a live claim, is not processed, and is retried after the lease; by then the chunk is DONE and it is skipped. Attempts stay at 1 per chunk.
- A "busy" retry is deliberately **not** acknowledged (acking could lose the chunk if the holder dies) and deliberately does not follow the backoff curve (so a slow chunk cannot push its own duplicates into the DLQ).

## Slow chunk (`heartbeatKeepsASlowChunkFromBeingReprocessed`, `controlWithoutHeartbeat...`)
- **Happens:** a chunk takes longer than the visibility timeout (the test uses 12s of work against a 5s timeout and 3s lease).
- **With heartbeat:** visibility and lease are extended every second; no other worker touches it (every chunk has exactly 1 attempt).
- **Control, heartbeat off:** another worker takes the chunk over and the slow worker later redoes it (attempts >= 2), yet counts stay exact: the claim and the transactional counter make the duplicate work harmless.
- **Splitter:** the same heartbeat keeps a long split's `file-events` message from being redelivered.

## Poison message (`poisonMessageGoesToDlqWithBackoffAndDoesNotBlockGoodWork`)
- **Happens:** a message that can never succeed (malformed JSON in the test).
- **Recovery:** retried with growing delays, moved to `chunk-jobs-dlq` after 5 receives. It does not block the queue: a valid job submitted alongside completes normally.
- **Why correct:** a poison message never reaches the DB or the counter. Without the backoff handler the same journey takes 5 x 120s.
- **Operator action:** inspect the DLQ, fix the cause, redrive (below).

## Missing S3 object (`missingS3ObjectGoesToDlqAndRedriveCompletesTheJobAfterTheFix`)
- **Happens:** a chunk object is gone. The worker throws `NoSuchKey`, releases its claim, the message backs off and lands in the DLQ after 5 receives.
- **State while parked:** the job stays `PROCESSING`, `chunksDone` is 3 of 4, the chunk is not DONE, nothing partial was written for it.
- **Recovery:** restore the object, redrive. The job completes with exact counts.
- A missing *raw* upload fails the same way on `file-events`.

## Database temporarily unavailable (`databaseOutageIsRetriedWithBackoffAndLosesNothing`)
- **Happens:** Postgres stops answering mid-run (the test pauses the container for 9s). Connections fail fast (5s pool timeout) or hang until the 30s socket timeout rather than blocking forever.
- **Recovery:** affected chunks fail, release their claims, back off and retry; the heartbeat keeps working chunks' messages invisible while they are still alive. Once the DB returns they succeed.
- **Why correct:** the chunk's DB work is one transaction, so a failure leaves nothing partial; the counter moves only after it commits.
- **Limit:** an outage longer than roughly 5 retries' worth of backoff (about 2 + 4 + 8 + 16 = 30s of delay plus the attempts) sends chunks to the DLQ. They are not lost: redrive them after the DB is back. Tune `retry-base-delay` / the queue's `maxReceiveCount` to match your outage tolerance.

## Concurrent chunks upserting the same trip (`PipelineIT`, ADR-006)
- **Happens:** duplicates span chunks, so parallel transactions touch the same rows. Unsorted, Postgres detected deadlocks.
- **Fix:** every chunk upserts in `trip_id` order, so locks are always taken in the same order. Deadlock remains a retryable error as a safety net.

## Duplicate or redelivered BatchCompleted (`FanOutIT`)
- **Happens:** the event reaches a consumer more than once (SNS/SQS are at-least-once, the publisher itself may re-publish after a crash).
- **Aggregator:** recomputes the affected vehicle-days from `trips`, so repeating it writes the same values; the `processed_events` row makes duplicates skip the work. The test delivers one job's event three times and then re-uploads the same trips as a second job: stats still equal an oracle computed straight from the CSV (nothing doubled).
- **Notifier:** each event id is claimed in DynamoDB before sending; duplicates see DONE and skip. One notification per job in the test despite 3 deliveries. A crash between send and mark-done can notify again after the lease, so delivery is at-least-once.
- **A consumer is down or failing:** its queue buffers events (the other consumer is unaffected); persistent failures go to that consumer's own DLQ and can be redriven with `scripts/redrive.sh aggregator-queue` (or `notifier-queue`).

## Known gaps (called out, not hidden)
- **Redrive endpoint has no authentication.** Keep it on a private network / behind IAM in AWS.
- **Splitter crash mid-split** re-splits from the start on redelivery (deterministic, idempotent), but rewrites every chunk object; very large files pay that cost twice.
- **A job whose DLQ'd chunk is never redriven stays `PROCESSING`.** The DLQ-depth alarm (Phase 5) is the signal; there is no automatic job timeout yet.
- **`good_rows` counts trips tagged with the job.** A later job re-uploading the same trip ids takes them over.

## Redrive procedure (DLQ to main queue)
1. **Detect:** `chunk-jobs-dlq` (or `file-events-dlq`) depth > 0 (CloudWatch alarm in Phase 5; locally `awslocal sqs get-queue-attributes`).
2. **Inspect:** receive a message from the DLQ and read the worker log lines containing the `jobId` / `chunkId`; the log shows the root cause of each failed attempt.
3. **Fix the cause** (restore the object, fix the code and redeploy, bring the DB back). Redriving without fixing just returns the message to the DLQ after 5 more receives.
4. **Redrive:**
   ```bash
   scripts/redrive.sh chunk-jobs http://localhost:8080        # or: POST /admin/redrive/chunk-jobs?max=1000
   ```
   Each message is sent back to the source queue, then deleted from the DLQ (send before delete: a crash can duplicate, never lose; consumers are idempotent). On real AWS the native equivalent is `aws sqs start-message-move-task --source-arn <dlq-arn>`.
5. **Verify:** `GET /jobs/{id}` reaches `COMPLETED`; DLQ depth is 0.
