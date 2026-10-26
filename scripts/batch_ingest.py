"""Batch ingestion: CSV mensimulasikan REST API ber-pagination."""
import argparse, json, random, time
from pathlib import Path
import pandas as pd

class TransientError(Exception):
    """Error sementara (mirip HTTP 429/503)."""

def get_page(df, page, page_size, retries=4, fail_rate=0.0, base_delay=0.5):
    for attempt in range(retries):
        try:
            if random.random() < fail_rate:           # simulasi kegagalan
                raise TransientError(f"simulasi gagal page={page}")
            start = page * page_size
            return df.iloc[start:start + page_size].copy()
        except TransientError as e:
            if attempt == retries - 1:
                raise
            wait = base_delay * (2 ** attempt)        # exponential backoff
            print(f"[retry] page={page} percobaan={attempt+1} tunggu={wait}s ({e})")
            time.sleep(wait)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/orders.csv")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--page-size", type=int, default=25)
    ap.add_argument("--checkpoint", default="data/watermark.json")
    ap.add_argument("--fail-rate", type=float, default=0.0)
    args = ap.parse_args()

    # Raw layer = apa adanya (string); kolom bantu hanya untuk watermark
    source = pd.read_csv(args.source, dtype=str, keep_default_na=False)
    source["order_ts_parsed"] = pd.to_datetime(source["order_ts"], errors="coerce", utc=True)

    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    checkpoint = Path(args.checkpoint)
    last_ts = None
    if checkpoint.exists():
        last_ts = json.loads(checkpoint.read_text()).get("watermark")

    pages, page = [], 0
    while True:
        chunk = get_page(source, page, args.page_size, fail_rate=args.fail_rate)
        if chunk.empty:
            break
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk); page += 1
    fetched = pd.concat(pages, ignore_index=True) if pages else source.iloc[0:0].copy()

    new_rows = fetched
    if last_ts:                                       # incremental loading
        new_rows = fetched[fetched["order_ts_parsed"] > pd.Timestamp(last_ts)]
    new_rows = new_rows.sort_values("order_ts_parsed")

    target = out / "orders.parquet"
    old = pd.read_parquet(target) if target.exists() else fetched.iloc[0:0].copy()
    merged = (pd.concat([old, new_rows], ignore_index=True)
                .drop_duplicates("order_id", keep="last"))   # idempoten
    merged.to_parquet(target, index=False)

    wm = merged["order_ts_parsed"].max()              # watermark ditulis SETELAH sukses
    if pd.notna(wm):
        checkpoint.write_text(json.dumps({"watermark": wm.isoformat()}, indent=2))

    appended = len(merged) - len(old)
    print(f"SUMMARY read={len(fetched)} after_watermark={len(new_rows)} "
          f"appended={appended} total={len(merged)}")

if __name__ == "__main__":
    main()