{{ config(order_by='(issued_date, card_type)') }}
-- Listed fees describe card contracts, not collected payment revenue.
select toDate(issued_date) as issued_date, card_type,
       count() as cards_issued, sum(annual_fee) as listed_annual_fees
from {{ ref('dim_cards') }}
group by issued_date, card_type
