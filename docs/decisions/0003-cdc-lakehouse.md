# ADR-0003: CDC and lakehouse pipeline

- Date: 2026-10-27
- Status: Accepted
- Supersedes ADR-0002 for the current implementation

## Decision

Earlier generator and connector prototypes are removed from the working tree;
they remain available in Git history.

Use PostgreSQL, Debezium, Kafka, Spark Structured Streaming, MinIO, Iceberg,
ClickHouse and dbt. Airflow orchestrates separate jobs through a small internal
job service. The job service runs one Spark process at a time.

Use a JDBC Iceberg catalog in the existing PostgreSQL service. This avoids
adding Hive, Hadoop or a REST catalog service. Table data and metadata files
are in MinIO, while catalog pointers are in PostgreSQL.

Use available-now streaming triggers: consume the currently available Kafka
records, checkpoint them and exit. The source connector runs continuously.
This gives bounded demonstrations without keeping a Spark JVM running all day.
It is Structured Streaming, but not an always-on low-latency deployment.

## Resource choices

One Kafka broker, one partition per topic, local Spark with two threads and
one active job. Superset and monitoring are optional Compose profiles.
Airflow uses SequentialExecutor and SQLite for this single-user local demo.
No Docker socket is mounted into Airflow or the worker.

## Alternatives

- Spark instead of Flink: one processing engine for batch and streaming.
- Iceberg instead of Delta/Hudi: one table format with snapshots and updates.
- MinIO instead of HDFS: one object store for local lake data.
- dbt tests plus explicit reconciliation instead of Great Expectations.
- JSON CDC envelopes with explicit Spark schemas instead of Avro/Schema Registry.
  There is no schema compatibility service in this version. New fields require
  a reviewed schema/code change; incompatible changes fail verification.
- dbt manifest and repository lineage documentation instead of DataHub.
  A dedicated metadata platform would add substantial local resource cost.

## Delivery semantics and limits

Raw events merge by topic, partition and offset, so retrying a microbatch does
not duplicate events. Current state is reconstructed using primary keys, source
LSN and event offsets, including deletes. Transactions are partitioned by UTC
transaction date. The selected date is replaced in Iceberg and ClickHouse.
ClickHouse partition replacement is atomic; the entire multi-system pipeline
is not one distributed transaction. Consumers should use successful Airflow runs.

A source barrier is inserted after seeding. Ingestion waits until that marker
has passed through Debezium and Kafka, then drains once more. This avoids a fixed
sleep being the only synchronization mechanism.

Small reference tables are rebuilt as snapshots. dbt rebuilds marts from all
loaded partitions. Reports are published after tests pass. The generator allows 1,000 to 100,000 transactions per day; validation uses
1,200. Larger volumes have not been benchmarked. Warehouse transfer collects
one date into the driver to keep the example short.
Dimension changes only refresh a date's enriched lake rows when that date is
rerun. A date-changing update requires rerunning both old and new dates. Kafka
retention is finite; checkpoint recovery beyond retention must fail rather than
silently skip records. Cross-partition transaction atomicity is not claimed.
