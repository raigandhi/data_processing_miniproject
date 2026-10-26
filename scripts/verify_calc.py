import polars as pl

df = pl.read_parquet("data/valid/orders_transformed.parquet")
expected = {"O1001": 237.5, "O1003": 35.073}

for order_id, exp in expected.items():
    row = df.filter(pl.col("order_id") == order_id).row(0, named=True)
    manual = row["quantity"] * row["unit_price"] * (1 - row["discount_pct"])
    ok = abs(row["net_amount"] - exp) < 1e-9
    print(f"{order_id}: qty={row['quantity']} harga={row['unit_price']} "
          f"diskon={row['discount_pct']} gross={row['gross_amount']} "
          f"net_program={row['net_amount']} net_manual={manual:.3f} harapan={exp} -> {'COCOK' if ok else 'TIDAK COCOK'}")