# ADR-0004: Observable timestamps and continuous microbatch processing

- Date: 2026-10-30
- Status: Accepted
- Extends ADR-0003; the batch path remains supported

## Decision

Carry source, CDC, Kafka, ingestion, transformation and warehouse timestamps,
plus topic/partition/offset/LSN provenance. Store instants in UTC. Keep txn_time
as business event time; do not confuse a historical logical date with the wall
clock when a record passed through the platform.

Configure business Kafka topics with LogAppendTime. Spark retains timestampType:
1 identifies broker append time; historical records with type 0 retain their
producer CreateTime meaning. Changing topic configuration does not rewrite old
records. CDC source.ts_ms provides source change/commit timing; payload ts_ms is
Debezium processing time. PostgreSQL also records created_at and updated_at for
new writes. Historical rows without source audit observations remain NULL.

Raw ingestion time is first persisted observation under the new instrumentation.
Replay fills previously uninstrumented raw records if they remain in Kafka, but
never replaces a known ingestion time on retry. Old Kafka records beyond
retention cannot have their missing transport time reconstructed. No historical
load time is fabricated: old ClickHouse rows receive NULL until reprocessed.
processed_at is captured after transformed data has been evaluated, before the
Iceberg commit. loaded_at is assigned by ClickHouse now64() during insertion
into the incoming warehouse table, before partition publication. These are
measured processing milestones, not a distributed transaction commit timestamp.

## Continuous mode

The existing worker owns one long-lived external spark-submit process. Its
Structured Streaming processing-time trigger remains active between arrivals.
foreachBatch merges raw events, rebuilds affected UTC date partitions, loads
ClickHouse, runs dbt and checks, then exports reports. A single Spark session is
reused. Reference changes invalidate enriched dates, and both before and after
transaction dates are rebuilt for date-moving updates. Empty deleted partitions
are cleared rather than leaving stale rows or stale CSV contents.

One writer lock excludes batch work while realtime owns the pipeline. Airflow
still contains no Spark/data logic; realtime is a separately controlled service
process. Batch CLI requests fail clearly instead of silently starting a second
Spark JVM. Read-only timing SQL does not require a new Spark process.

The trigger interval is a target, not a latency SLA. A heavy microbatch can take
longer; commits are not per-record instantaneous. The first replay/catch-up may
be considerably slower than subsequent updates. The stream requires an initial
successful batch bootstrap. During continuous source updates quality compares
the materialized snapshot with the marts, not the moving operational database.
After the producer stops and processing catches up, stop realtime and use the
strict source reconciliation command.

## Recovery and limits

Checkpoint commit follows successful sink work. A failure can leave a newer raw
snapshot or warehouse partition before the callback is committed. Replaying the
same offsets is safe at the business-row level. This is at-least-once execution
with idempotent partition publication, not global exactly-once or cross-topic
transaction isolation. Warehouse timestamps record a new load attempt on retry.

Graceful stop waits for a processing gap; allow the producer to finish first.
An interrupted worker marks the process interrupted/failed. Restart continuous
mode explicitly to resume its persistent checkpoint. Distinct batch/realtime
checkpoints can reread records; raw event keys prevent duplication.

This remains a small single-host teaching platform. Real-time means continuously
running microbatch processing with measured delay, not a hard sub-second SLA.

Receipt timing is captured after the Kafka batch has been read into Spark cache,
rather than using trigger-start time, which can precede a record fetched during
offset discovery. It is a microbatch receipt observation, not a per-byte network
arrival measurement. On facts, source_changed_at belongs to the transaction CDC
version; dimension-only rebuilds can update processing/load times without changing
that transaction source timestamp.
