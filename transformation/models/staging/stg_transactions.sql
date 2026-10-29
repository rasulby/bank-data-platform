select txn_id, card_id, customer_id, card_type, trimBoth(merchant) as merchant,
       amount, currency, status, txn_time, txn_date,
       source_changed_at, cdc_emitted_at, kafka_timestamp, kafka_timestamp_type,
       ingested_at, processed_at, loaded_at,
       source_topic, source_partition, source_offset, source_lsn, cdc_operation
from {{ source('bank_raw', 'raw_transactions') }}
