# TripStream

Event-driven trip-processing pipeline on AWS (S3, SQS, SNS, DynamoDB) with Spring Boot 3 / Java 21 and PostgreSQL.
A client uploads a large CSV of synthetic trips; the system validates it, processes it in parallel chunks, stores clean
rows, reports bad rows, and fans out events. Every stage is idempotent and retry-safe.

> Status: Phase 0 (repo and tooling). See the build plan in the project brief.

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

## Layout
- `src/main/java/com/tripstream/roles` - per-profile role wiring
- `src/main/resources/db/changelog` - Liquibase schema
- `infra/localstack/init-aws.sh` - idempotent creation of all local AWS resources
- `docs/decisions.md`, `docs/failure-modes.md` - ADRs and failure analysis

## Architecture
_Diagram and trade-offs added in Phase 7._
