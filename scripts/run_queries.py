import duckdb

con = duckdb.connect("warehouse/warehouse.duckdb", read_only=True)

queries = {
    "1. Penjualan bersih per hari":
        "SELECT order_date, SUM(net_sales) AS net_sales FROM mart_daily_sales GROUP BY 1 ORDER BY 1",
    "2. Kategori dengan penjualan bersih tertinggi":
        "SELECT product_category, SUM(net_sales) AS net_sales FROM mart_daily_sales GROUP BY 1 ORDER BY 2 DESC",
    "3. Jumlah dan nilai pesanan menurut status":
        "SELECT status, COUNT(*) AS order_count, SUM(net_amount) AS net_value FROM fct_orders GROUP BY 1 ORDER BY 2 DESC",
    "4. Metode pembayaran terbanyak":
        "SELECT payment_method, COUNT(*) AS n FROM fct_orders GROUP BY 1 ORDER BY 2 DESC",
    "5. Record ditolak dan penyebabnya":
        "SELECT split_part(e, ':', 1) AS field, COUNT(*) AS n "
        "FROM (SELECT unnest(errors) AS e FROM read_json_auto('data/rejected/rejected_orders.jsonl')) "
        "GROUP BY 1 ORDER BY 2 DESC",
}

for title, sql in queries.items():
    print(f"\n=== {title} ===")
    print(con.execute(sql).fetchdf().to_string(index=False))

con.close()