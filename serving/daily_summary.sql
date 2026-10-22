-- Schema name comes from SERVING_SCHEMA. Default: serving.
SELECT txn_date, currency, transaction_count, total_amount, declined_count
FROM serving.daily_summary
ORDER BY txn_date, currency;
