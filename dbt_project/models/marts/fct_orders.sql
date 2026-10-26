-- fct_orders.sql  (satu baris per order_id)
select order_id, customer_id, product_category, order_ts, order_date,
       payment_method, status, quantity, unit_price, discount_pct,
       gross_amount, net_amount
from {{ ref('stg_orders') }}