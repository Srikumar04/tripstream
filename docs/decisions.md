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
