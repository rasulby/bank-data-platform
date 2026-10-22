# ADR-0002: Use a smaller stack for Phase 1

- Status: Accepted
- Date: 2026-10-22
- Replaces the Phase 1 stack in ADR-0001

## Decision

Use PostgreSQL for the source and warehouse, Python for CSV ingestion and
reports, Airflow for orchestration, and Docker Compose for local services.
Store raw CSV files in a persistent volume. Keep raw, staging, curated and
serving data in separate PostgreSQL schemas.

The consumer is an operations analyst who reads a daily SQL summary or CSV
report. Airflow uses SequentialExecutor and SQLite for this local setup.

## Reason

Phase 1 needs one working batch pipeline. Using fewer services makes it easier
to run locally and check each stage. A separate object store, warehouse engine
and dashboard are not needed for this version.

## Limitations

The source contains 1,200 synthetic transactions per day. Transformations are
basic, and source and warehouse share one PostgreSQL instance.

MinIO, ClickHouse, dbt, Grafana, Kafka and Debezium are possible Phase 2 additions.
They are outside the current architecture. The older CDC files remain in the
repository, but Compose does not start them.
