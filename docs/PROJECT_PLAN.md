# Bank Data Platform Plan

Build a local CDC and batch analytics platform for synthetic retail-bank data.
The operations analyst needs daily volume, declines, issued cards and customer
segments. The demo uses customers, cards and transactions, with a repeatable
source insert/update example.

| Technology | Role |
|---|---|
| Bash, Git, Docker Compose | Commands, version control and local services |
| Python and SQL | Source generator, loading, tests and analytics |
| PostgreSQL | Operational source and Iceberg JDBC catalog |
| Debezium | Snapshot and WAL-based CDC |
| Kafka | Durable change-event broker |
| Kafbat UI | Inspect live topic messages and Debezium connector status |
| Spark | Bounded or continuous Structured Streaming and batch joins |
| MinIO | Object store |
| Parquet and Iceberg | Columnar data files and lakehouse tables |
| ClickHouse | Analytical warehouse |
| dbt | Staging, fact/dimension models, tests and model lineage |
| Superset | Optional business dashboard |
| Prometheus and Grafana | Optional job monitoring |
| GitHub Actions | Static CI checks |

HDFS, Flink, Delta Lake and Hudi are alternatives rather than extra components.
Data quality uses dbt and explicit reconciliation, without Great Expectations.
Schema Registry and DataHub are not deployed: JSON schemas are explicit in the
Spark code, and lineage is documented in the repo and dbt manifest. These limits
are intentional for a 16 GB single-machine demonstration.

## Changelog

- 2026-10-14: Initial architecture.
- 2026-10-22: Small batch-only implementation; historical Phase 1 evidence retained.
- 2026-10-27: CDC, separate Spark jobs, Iceberg, ClickHouse and dbt implementation;
  optional BI and monitoring profiles. See ADR-0003.

- 2026-10-30: Added row-level UTC timing/provenance, source audit columns,
  and controlled continuous microbatches with writer exclusion (ADR-0004).
