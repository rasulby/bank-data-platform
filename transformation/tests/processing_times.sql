-- Legacy rows with no recorded ingestion time are not assigned invented times.
-- Every row instrumented by the current pipeline must have a valid processing chain.
select txn_id
from {{ ref('fct_transactions') }}
where ingested_at is not null
  and (processed_at is null or loaded_at is null or kafka_timestamp is null
       or cdc_emitted_at is null
       or processed_at < ingested_at or loaded_at < processed_at)
