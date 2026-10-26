-- dim_product_category.sql
select distinct product_category from {{ ref('stg_orders') }}