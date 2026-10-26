import polars as pl
from pathlib import Path

batch = pl.read_parquet("data/valid/valid_orders.parquet")
frames = [batch]
stream_path = Path("data/stream/valid_events.jsonl")
if stream_path.exists():
    frames.append(pl.read_ndjson(stream_path).select(batch.columns))

df = (
    pl.concat(frames)
    .unique(subset="order_id", keep="last")           # gabungan batch+stream tidak ganda
    .with_columns(
        pl.col("order_ts").str.to_datetime(time_zone="UTC").dt.replace_time_zone(None),
        pl.col("quantity").cast(pl.Int32),
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    )
    .with_columns((pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"))
    .sort("order_id")
)
print(df.schema)                                      # periksa tipe data
df.write_parquet("data/valid/orders_transformed.parquet")
print(df.head(5))