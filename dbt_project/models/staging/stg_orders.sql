select
    order_id, customer_id,
    cast(order_ts as timestamp) as order_ts,
    cast(order_ts as date)      as order_date,
    product_category, quantity, unit_price, discount_pct,
    payment_method, status, gross_amount, net_amount
from {{ source('raw', 'raw_orders') }}