# Benchmarks and chaos results

**Read this first.** Everything here was measured on one laptop (macOS, 14 logical CPUs, 24 GB RAM) running Postgres 16
(default settings, in Docker), **LocalStack 3.8.1 as an emulator of S3/SQS/SNS/DynamoDB**, and the TripStream JVMs
(Java 21, workers `-Xmx384m`). The absolute numbers are therefore *not* AWS numbers: real SQS/S3/DynamoDB do not behave
like LocalStack, and RDS is not a Docker container on the same host. What transfers is the *shape*: how the pipeline
scales, where it stops scaling and why, and the correctness-under-failure results. Each number below is a single run
(no confidence intervals); the harness is in `scripts/bench/bench.py` and raw results in
`docs/benchmark-results.json`. Unrelated idle containers were also running on the machine.

Setup unless stated otherwise: 5,000-row chunks, one worker process = one chunk at a time (concurrency 1), data from
`scripts/generate_trips.py` with 5% bad rows and 2% duplicates. **Every run below ended on the exact good/bad counts from
the generator's manifest** (no loss, no double counting); runs that did not are called out.

## Headline numbers

| What | Result |
|---|---|
| 5,000,000-row file, 4 workers | **75.0 s end to end, 66,692 rows/s** (1,000 chunks), exact counts |
| Scaling 1 -> 4 workers (1M rows) | 33,205 -> **67,901 rows/s (2.05x)**; 8 workers gave 63,474 (no further gain) |
| Why it stopped scaling | row-lock contention between concurrent chunk transactions (74% of active DB sessions waiting on `Lock:transactionid` at 8 workers), not CPU |
| After removing the contention (commit per batch) | 4 workers: 67,901 -> **103,663 rows/s (+53%)** |
| + JDBC `reWriteBatchedInserts`, 8 workers | **115,216 rows/s** (1M rows in 8.7 s), chunk p99 0.65 s |
| Chunk duration (1M rows, 4 workers) | p50 0.20 s / p95 0.58 s / p99 0.73 s |
| Chaos: 4 x `kill -9` of workers mid-run + 35% of chunk messages duplicated (1M rows) | **exact counts**, DynamoDB `chunksDone` 200/200, DLQ empty |
| Poison flood: 300k all-bad rows + 2 garbage files next to a legit job | bad input contained, legit job exact, nothing parked in a DLQ |

## 1. Throughput by file size (4 workers)

| Rows | Chunks | End to end | Rows/s | Chunk p50 / p95 / p99 | First chunk starts after |
|---|---|---|---|---|---|
| 100,000 | 20 | 2.6 s | 39,046 | 0.26 / 0.78 / 0.81 s | 0.5 s |
| 1,000,000 | 200 | 14.7 s | 67,901 | 0.20 / 0.58 / 0.73 s | 0.3 s |
| 5,000,000 | 1,000 | 75.0 s | 66,692 | 0.22 / 0.52 / 0.71 s | 0.5 s |

End to end = from the moment the upload finished until the job reported `COMPLETED`. Throughput is flat from 1M to 5M
rows, so nothing degrades with size (the splitter streams the file and workers start on the first chunk within 0.5 s;
nothing is held in memory). Chunk duration is `finished_at - started_at` from `job_chunks`: claim to DB commit, i.e.
parse + validate + upsert, excluding queue wait. "DB rows/s" in the raw JSON is valid rows divided by the first-start to
last-finish window and tracks the end-to-end figure closely.

## 2. Scaling workers (1,000,000 rows)

| Workers | Rows/s | Speedup | Chunk p50 / p95 / p99 | Total worker-busy time | Postgres CPU (avg / max cores) |
|---|---|---|---|---|---|
| 1 | 33,205 | 1.00x | 0.12 / 0.14 / 0.16 s | 24.5 s | 0.45 / 0.67 |
| 2 | 54,818 | 1.65x | 0.16 / 0.24 / 0.28 s | 30.7 s | 1.02 / 1.13 |
| 4 | 67,901 | 2.05x | 0.20 / 0.58 / 0.73 s | 51.8 s | 1.28 / 1.56 |
| 8 | 63,474 | 1.91x | 0.41 / 1.52 / 1.99 s | 112.2 s | 0.93 / 1.75 |

At 8 workers the same work takes 112 worker-seconds instead of 25: workers spend most of their time waiting, p99 chunk
time triples, and throughput is no better than at 4. Postgres uses about one core, so the database is not CPU-bound.
(JVM CPU is not reported: the sampler's per-process figure is a lifetime average and is not meaningful here.)

### Where it stops scaling, and why

Sampling `pg_stat_activity` every second during the run showed what active Postgres sessions were waiting on:

| Run | `Lock:transactionid` | Running on CPU | Other |
|---|---|---|---|
| 2 workers | 36% | 55% | WALWrite 9% |
| **8 workers** | **74%** | 26% | |

`Lock:transactionid` means "waiting for another transaction to commit so this row can be touched". Each chunk is one
transaction holding row locks until it commits, and duplicate trips span chunks that are in flight at the same time, so
an upsert of an already-inserted-but-uncommitted `trip_id` blocks behind the other chunk's transaction (sorting rows by
`trip_id`, ADR-006, prevents deadlocks but not these waits). More concurrent chunks mean longer waiting chains, which is
exactly the growth in busy time and p99 above.

Two experiments tested that explanation instead of assuming it:

| Experiment | 4 workers | 8 workers | What the DB was doing |
|---|---|---|---|
| Baseline (2% duplicates) | 67,901 | 63,474 | 74% lock waits at 8 workers |
| **Control: same file with 0% duplicates** | **109,160** | 102,009 | lock waits gone: 100% CPU at 4 workers; at 8, relation-extend and data-file-extend waits |
| **Mitigation: commit every batch, not every chunk** (`tripstream.worker.commit-per-batch=true`) | **103,663** | 92,710 | no lock waits; at 8, WALWrite 36% |
| Mitigation + `reWriteBatchedInserts` | n/a | **115,216** | CPU 90%, WALSync 10% |

Removing the duplicates, or shortening how long locks are held, removes the contention and 4 workers jump from 68k to
104-109k rows/s, so duplicates held in long transactions were the limiter. Past that, the ceiling (about 100-115k rows/s
here) is the Postgres write path itself: WAL writes/syncs, extending the table and four indexes per row, on a single
instance in a Docker VM. Adding workers beyond about 4 cannot help until the database gets faster storage or the schema
gets fewer indexes, and on RDS the equivalent step is a larger instance or provisioned IOPS.

**Trade-off of commit-per-batch (off by default).** Each 500-row batch commits separately, so a crash mid-chunk leaves
some of the chunk's trips already written. That is harmless: the upsert is idempotent, the retry rewrites the same rows as
no-ops, the job totals are computed only after every chunk is DONE, and the chunk itself is marked done only after the
last batch. The cost is that the "nothing partial is ever visible" property in `docs/failure-modes.md` no longer holds
for the `trips` table while a chunk is running. It is verified by the exact-count checks above and by the chaos run in
section 4, but the integration suite (`FailureModesIT`) runs against the default.

## 3. Tuning experiments (1,000,000 rows, 4 workers)

| Change | Rows/s | Chunk p99 | Note |
|---|---|---|---|
| Baseline | 67,901 | 0.73 s | |
| `reWriteBatchedInserts=true` (pgjdbc rewrites batched inserts into multi-row inserts) | **81,962 (+21%)** | 0.68 s | not enabled by default yet |
| `commit-per-batch=true` | 103,663 (+53%) | 0.32 s | see trade-off above |

## 4. Chaos at scale

**Kill workers mid-run, with duplicates injected** (1,000,000 rows, 200 chunks, 4 workers, queue visibility 20 s and claim
lease 15 s so recovery takes seconds):

- Four `SIGKILL`s: two workers at 57/200 chunks done (6 s in), two more at 116/200 (11 s in); each time replacements were
  started as an orchestrator would.
- 70 duplicate chunk messages (35% of chunks) sent while the job ran.
- Result: job `COMPLETED` with exactly 1,000,000 total / 930,000 good / 50,000 bad; trips table 930,000 rows; every
  chunk DONE; **DynamoDB `chunksDone` = 200 = chunk count** (none counted twice, none missed); no chunk needed more than 2 attempts;
  DLQ empty. Completion took about 35 s versus 14.7 s without chaos, i.e. the cost of two lease expiries plus
  JVM restarts, not data loss. (The run then waited 30 s to confirm late duplicates changed nothing.)

**Same chaos with `commit-per-batch=true`**, the case where a killed worker has already committed some of its chunk's
batches: workers killed at 66/200 and 118/200 chunks done, 70 duplicate messages, exact counts again, `chunksDone` 200/200,
no chunk needed more than 2 attempts, DLQ empty. This is the evidence that partially committed chunks are safe.

**Poison flood.** Uploaded at the same time, with 4 workers:

| File | Outcome |
|---|---|
| 300,000 rows, every row malformed | `COMPLETED`: total 300,000, good 0, bad 300,000 (in 2.7 s), error reports written |
| Wrong header + 50,000 junk lines | `FAILED` cleanly (2.7 s), nothing enqueued |
| 5 MB of random bytes | `FAILED` cleanly (2.7 s) |
| Legitimate 100,000-row job (same moment) | `COMPLETED` exact (93,000 good / 5,000 bad) in 4.3 s |

The trips table held only the legit job's 93,000 rows, and both DLQs stayed empty: bad data is handled as data (error
report, job counts), not as failing messages, so it neither stalls other work nor parks anything.

## 5. Things the benchmarking found and fixed

1. **Chunk durations were wrong** (found by sanity-checking a 20 ms p50): `finished_at = now()` inside the DB
   transaction returns the transaction's *start* time in Postgres, so upsert time was excluded. Now `clock_timestamp()`.
2. **Postgres connection exhaustion at 8 workers** (`too many clients`): every JVM's default pool is 10 connections, and
   api + splitter + 8 workers exceeded `max_connections` (100). The pool size is now configurable (`DB_POOL_SIZE`);
   rule of thumb: instances x pool size must stay under the database's limit, and a worker needs about
   `concurrency + 2`.
3. **A killed consumer can hold a message hostage** (harness artifact, same mechanism as Phase 3): two scaling runs
   took 121 s instead of 18 s because the previous run's `SIGKILL`ed workers left SQS long polls registered with the
   broker, which swallowed the next messages until their 120 s visibility timeout. Evidence: workers busy 32 s out of a
   120 s window, no retries, no failures. The harness now waits for stale polls to expire; the re-run confirmed it. In
   production this is the visibility-timeout recovery time after a crash, which is why the timeout and heartbeat are
   tuned together (`docs/failure-modes.md`).
4. **Lock contention between chunk transactions** (section 2).

## 6. Cost estimate (arithmetic, not a measurement)

Request counts per 1,000,000 rows (200 chunks), from the code: S3 about 401 PUTs and 201 GETs; SQS about 610 requests;
DynamoDB about 1,000 write units and 200 strongly-consistent read units; 1 SNS publish. At approximate us-east-1 on-demand
list prices (S3 $0.005 per 1k PUT, $0.0004 per 1k GET; SQS $0.40 per M; DynamoDB on-demand $1.25 per M writes, $0.25 per
M reads; **verify current pricing**) that is roughly **$0.004 of messaging/storage requests per million rows**: negligible.

Compute is what is left. If one Fargate worker (1 vCPU, 2 GB, about $0.05/h) sustains the ~33k rows/s that one worker
process managed here, a million rows is about 30 s of worker time, about **$0.0004**; the managed database is a fixed
hourly cost (an always-on RDS instance), not a per-row cost. Order of magnitude: a cent or less per million rows of
marginal cost, dominated by the database, and a 5M-row file costs well under a cent in requests. These assume the local
per-worker speed carries over to Fargate and RDS, which has not been measured.

## 7. Limits of this evidence

- One machine, one run per configuration, emulated AWS services; numbers will differ on AWS.
- Postgres was untuned (default `shared_buffers`, WAL settings, synchronous commit on) and shared a host with the workers.
- The aggregator and notifier roles were not running during the throughput runs (they work off the completion event,
  after the measured window).
- Chunk size was not swept; 5,000 rows was chosen up front. Chunks of about 0.1-0.3 s keep per-message overhead small and
  recovery from a killed worker cheap.

## Reproduce

```bash
docker compose up -d --wait            # POSTGRES_PORT=... LOCALSTACK_PORT=... if the defaults clash
mvn -DskipTests -DskipITs package
python3 scripts/bench/bench.py size  --rows 5000000 --workers 4
python3 scripts/bench/bench.py scale --rows 1000000 --workers 1,2,4,8
python3 scripts/bench/bench.py scale --rows 1000000 --workers 4,8 --label commit-per-batch \
        --worker-arg=--tripstream.worker.commit-per-batch=true
python3 scripts/bench/bench.py chaos  --rows 1000000 --workers 4
python3 scripts/bench/bench.py poison
python3 scripts/bench/bench.py report
```
