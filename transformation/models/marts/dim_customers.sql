{{ config(order_by='customer_id') }}
select customer_id, full_name, city from {{ source('bank_raw', 'customers') }}
