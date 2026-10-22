# Bank Data Platform Plan

Build a daily batch pipeline for synthetic bank transactions.
The consumer is an operations analyst who needs transaction counts, total
amounts and declined transaction counts.

| Component | Role | Status |
|---|---|---|
| PostgreSQL public.batch_transactions | Synthetic source data | Implemented |
| Python and CSV volume | Ingestion and raw files | Implemented |
| PostgreSQL raw/staging/curated | Warehouse layers | Implemented |
| PostgreSQL serving and CSV | Daily summary and report | Implemented |
| Airflow | Task dependencies, dates, retries and schedule | Implemented |
| Docker Compose | Local infrastructure | Implemented |
| GitHub Actions | Lint, format, compile and Compose checks | Implemented |
| Kafka, Debezium, MinIO, ClickHouse, dbt, Grafana | Possible extensions | Phase 2 |

Run results are recorded in [phase-1.md](evidence/phase-1.md).

## Changelog

- 2026-10-14: Initial plan.
- 2026-10-22: Reduced the Phase 1 stack to simplify local setup and testing.
  See [ADR-0002](decisions/0002-minimal-phase-1.md).
