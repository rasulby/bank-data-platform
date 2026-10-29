{{ config(order_by='card_id') }}
select card_id, customer_id, card_type, issued_date, toDecimal64(annual_fee, 2) as annual_fee
from {{ source('bank_raw', 'cards') }}
