{{ config(order_by='(txn_date, txn_id)') }}
select * from {{ ref('stg_transactions') }}
