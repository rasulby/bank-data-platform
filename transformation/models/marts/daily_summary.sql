select txn_date, currency, count() as transaction_count,
       sum(amount) as total_amount,
       countIf(status = 'DECLINED') as declined_count,
       round(100.0 * declined_count / count(), 2) as decline_rate_pct,
       max(source_changed_at) as latest_source_changed_at,
       max(loaded_at) as latest_loaded_at
from {{ ref('fct_transactions') }}
group by txn_date, currency
