import argparse, os, platform, statistics, time
from pathlib import Path
import numpy as np, pandas as pd, polars as pl

def make_datasets(copies):
    base = pd.read_parquet("data/valid/valid_orders.parquet")
    big = pd.concat([base] * copies, ignore_index=True)
    big["order_id"] = [f"B{i}" for i in range(len(big))]
    big.to_csv("data/bench/orders_big.csv", index=False)
    skew = big.copy()
    mask = np.random.default_rng(42).random(len(skew)) < 0.8      # 80% -> satu customer
    skew.loc[mask, "customer_id"] = "C001"
    skew.to_csv("data/bench/orders_skew.csv", index=False)
    return len(big)

def pandas_job(path, key):
    df = pd.read_csv(path)
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
    return df.groupby(key)["net_amount"].sum()

def polars_eager(path, key):
    df = pl.read_csv(path).with_columns(
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"))
    df = df.with_columns((pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"))
    return df.group_by(key).agg(pl.col("net_amount").sum())

def polars_lazy(path, key):
    return (pl.scan_csv(path)
            .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"))
            .with_columns((pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"))
            .group_by(key).agg(pl.col("net_amount").sum())
            .collect())

def timeit(fn, *a, repeat=5):
    fn(*a)                                   # warm-up, tidak dihitung
    ts = []
    for _ in range(repeat):
        t = time.perf_counter(); fn(*a); ts.append(time.perf_counter() - t)
    return statistics.median(ts)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--copies", type=int, default=20000)
    n = make_datasets(ap.parse_args().copies)
    print(f"rows={n} cpu={os.cpu_count()} os={platform.platform()} py={platform.python_version()}")
    for label, path in [("normal", "data/bench/orders_big.csv"), ("skew", "data/bench/orders_skew.csv")]:
        dist = pd.read_csv(path)["customer_id"].value_counts()
        print(f"[{label}] grup={len(dist)} terbesar={dist.iloc[0]} baris ({dist.iloc[0]/n:.0%})")
        for name, fn in [("pandas", pandas_job), ("polars_eager", polars_eager), ("polars_lazy", polars_lazy)]:
            print(f"  {name:13s} median={timeit(fn, path, 'customer_id'):.3f}s")