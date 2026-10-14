# ADR-0001: Batch-first Phase 1; CDC and streaming deferred to Phase 2

- Status: Accepted
- Date: 2026-10-14

## Context

The platform contains streaming components (Kafka, Debezium) that were built
first. Phase 1 requires a batch walking skeleton that runs end to end and is
re-runnable by logical date. Streaming pipelines belong to Phase 2.

## Decision

- Phase 1 pipeline is batch: PostgreSQL -> Python ingestion -> MinIO (Parquet)
  -> ClickHouse -> dbt -> Grafana, orchestrated by Airflow.
- Kafka is covered in Phase 1 by a scripted connectivity test only
  (produce a message, consume it back).
- Debezium CDC infrastructure stays in the repository and is verified to run,
  but it is not part of the Phase 1 data path. CDC, Spark Structured Streaming
  and Iceberg are Phase 2 work.

## Consequences

- Phase 1 stays small enough to be reliable and re-runnable.
- Kafka and Debezium containers are not needed for the batch pipeline and can
  be excluded from the default start-up to save resources.
