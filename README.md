# TripStream

Event-driven trip-processing pipeline on AWS (S3, SQS, SNS, DynamoDB) with Spring Boot 3 / Java 21 and PostgreSQL.
A client uploads a large CSV of synthetic trips; the system validates it, processes it in parallel chunks, stores clean
rows, reports bad rows, and fans out events. Every stage is idempotent and retry-safe.

> Status: Phase 1 (ingestion) done. See the build plan in the project brief.

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
mvn spring-boot:run -Dspring-boot.run.profiles=splitter     # in a second terminal

python3 scripts/generate_trips.py --rows 100000 --bad-pct 5 --dup-pct 2 --out data/generated/trips-100k.csv
scripts/upload.sh data/generated/trips-100k.csv             # POST /uploads, PUT to the presigned URL, poll the job
```

- `POST /uploads` creates a job (`CREATED`) and returns `{jobId, key, uploadUrl, expiresAt}`; PUT the CSV to `uploadUrl`.
- S3 `ObjectCreated` on `uploads/` publishes to `file-events`; the splitter role marks the job `RECEIVED`.
- `GET /jobs/{id}` returns status and counts (404 unknown, 400 malformed id).
- The generator writes `<out>.manifest.json` with the exact expected good/bad/duplicate counts, used for Phase 2 assertions.

## Layout
- `src/main/java/com/tripstream/roles` - per-profile role wiring
- `src/main/java/com/tripstream/jobs` - upload and job-status API; `.../ingest` - S3 event consumer
- `scripts/` - synthetic data generator and upload helper
- `src/main/resources/db/changelog` - Liquibase schema
- `infra/localstack/init-aws.sh` - idempotent creation of all local AWS resources
- `docs/decisions.md`, `docs/failure-modes.md` - ADRs and failure analysis

## Architecture
_Diagram and trade-offs added in Phase 7._
