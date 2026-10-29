-- ClickHouse. Times are UTC, stored with millisecond precision in the warehouse.
-- txn_time is business event time, not CDC or ingestion time.
-- kafka_timestamp_type: 0=producer CreateTime, 1=broker LogAppendTime.
SELECT txn_id, amount, txn_time,
       source_changed_at, cdc_emitted_at, kafka_timestamp, kafka_timestamp_type,
       ingested_at, processed_at, loaded_at,
       dateDiff('millisecond', source_changed_at, cdc_emitted_at) AS capture_delay_ms,
       dateDiff('millisecond', kafka_timestamp, ingested_at) AS kafka_to_spark_ms,
       dateDiff('millisecond', source_changed_at, loaded_at) AS source_to_current_load_ms,
       source_topic, source_partition, source_offset, source_lsn, cdc_operation
FROM bank.fct_transactions
WHERE txn_date = '2026-01-30'
ORDER BY source_changed_at DESC, txn_id
LIMIT 20;

-- Freshness of the materialized fact, not a promise of a fixed latency SLA.
SELECT max(source_changed_at) AS latest_source_change,
       max(loaded_at) AS latest_warehouse_load,
       countIf(ingested_at IS NULL) AS legacy_rows_without_instrumentation
FROM bank.fct_transactions;

-- Explicit UTC strings if the SQL client's display timezone is configured differently.
SELECT txn_id,
       toString(txn_time, 'UTC') AS event_time_utc,
       toString(source_changed_at, 'UTC') AS source_changed_utc,
       toString(ingested_at, 'UTC') AS ingested_utc,
       toString(processed_at, 'UTC') AS processed_utc,
       toString(loaded_at, 'UTC') AS loaded_utc
FROM bank.fct_transactions
WHERE merchant = 'Live demo'
ORDER BY source_changed_at DESC LIMIT 10;
