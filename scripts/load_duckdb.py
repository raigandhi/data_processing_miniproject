import duckdb

con = duckdb.connect("warehouse/warehouse.duckdb")
con.execute("""
CREATE TABLE IF NOT EXISTS raw_orders (
  order_id VARCHAR PRIMARY KEY, customer_id VARCHAR, order_ts TIMESTAMP,
  product_category VARCHAR, quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE,
  payment_method VARCHAR, status VARCHAR, gross_amount DOUBLE, net_amount DOUBLE)
""")
con.execute("""
INSERT OR REPLACE INTO raw_orders
SELECT order_id, customer_id, order_ts, product_category, quantity, unit_price,
       discount_pct, payment_method, status, gross_amount, net_amount
FROM read_parquet('data/valid/orders_transformed.parquet')
""")
print(con.execute("SELECT count(*) FROM raw_orders").fetchone())
con.close()          # WAJIB tutup, supaya dbt bisa membuka file ini