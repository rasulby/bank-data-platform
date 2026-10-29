-- ClickHouse: change the database name if CLICKHOUSE_DB is not bank.
SELECT txn_date, currency, transaction_count, total_amount,
       declined_count, decline_rate_pct
FROM bank.daily_summary
ORDER BY txn_date, currency;
