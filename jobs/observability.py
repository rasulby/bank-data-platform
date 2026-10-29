"""Shared row-level timing and lineage columns (all timestamps are UTC)."""

OBSERVATIONS = {
    "source_changed_at": "TIMESTAMP",
    "cdc_emitted_at": "TIMESTAMP",
    "kafka_timestamp": "TIMESTAMP",
    "kafka_timestamp_type": "INT",
    "ingested_at": "TIMESTAMP",
}
LINEAGE = {
    **OBSERVATIONS,
    "source_topic": "STRING",
    "source_partition": "INT",
    "source_offset": "BIGINT",
    "source_lsn": "BIGINT",
    "cdc_operation": "STRING",
    "processed_at": "TIMESTAMP",
}
