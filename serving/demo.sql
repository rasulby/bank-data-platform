-- PostgreSQL connection: localhost:5434, database/user/password from .env.
SELECT * FROM bank.customers ORDER BY customer_id LIMIT 10;
SELECT * FROM bank.cards ORDER BY card_id LIMIT 10;
SELECT * FROM bank.transactions ORDER BY txn_id LIMIT 10;
SELECT (txn_time AT TIME ZONE 'UTC')::date AS txn_date,
       count(*) AS transaction_count, sum(amount) AS total_amount
FROM bank.transactions GROUP BY 1 ORDER BY 1;

-- ClickHouse connection: localhost:8123, database/user/password from .env.
-- Run these in a SEPARATE ClickHouse SQL editor, not the PostgreSQL editor.
SELECT * FROM bank.dim_customers LIMIT 10;
SELECT * FROM bank.dim_cards LIMIT 10;
SELECT txn_date, count() AS row_count, uniqExact(txn_id) AS unique_ids
FROM bank.fct_transactions GROUP BY txn_date ORDER BY txn_date;
SELECT * FROM bank.daily_summary ORDER BY txn_date, currency;
SELECT txn_id, amount, merchant FROM bank.fct_transactions
WHERE merchant = 'CDC demo' OR amount = 999.00 ORDER BY txn_id;
SELECT * FROM bank.card_issuance_summary ORDER BY issued_date, card_type;

-- PostgreSQL: compare business time with actual source audit times.
SELECT txn_id, amount, txn_time, created_at, updated_at
FROM bank.transactions ORDER BY updated_at DESC NULLS LAST LIMIT 10;

-- ClickHouse: show the actual processing path and measured elapsed time.
-- More queries: serving/timings.sql.
SELECT txn_id, amount, txn_time, source_changed_at, cdc_emitted_at,
       kafka_timestamp, ingested_at, processed_at, loaded_at,
       dateDiff('millisecond', source_changed_at, loaded_at) AS source_to_current_load_ms
FROM bank.fct_transactions ORDER BY source_changed_at DESC LIMIT 10;
