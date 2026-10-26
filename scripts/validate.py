import argparse, json, sys
from collections import Counter
from pathlib import Path
import pandas as pd
from pydantic import ValidationError
from contract import Order

def validate_records(records):
    valid, rejected = [], []
    for rec in records:                                # satu per satu
        try:
            valid.append(Order.model_validate(rec).model_dump(mode="json"))
        except ValidationError as e:
            rejected.append({
                "order_id": rec.get("order_id") or None,
                "payload": rec,                        # data asal
                "errors": [f"{'.'.join(map(str, err['loc']))}: {err['msg']}"
                           for err in e.errors()],     # alasan
            })
    return valid, rejected

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw/orders.parquet")
    ap.add_argument("--fail-fast", action="store_true")
    args = ap.parse_args()

    df = pd.read_parquet(args.raw).drop(columns=["order_ts_parsed"])
    valid, rejected = validate_records(df.to_dict("records"))

    Path("data/rejected").mkdir(parents=True, exist_ok=True)
    Path("data/valid").mkdir(parents=True, exist_ok=True)
    with open("data/rejected/rejected_orders.jsonl", "w", encoding="utf-8") as f:
        for r in rejected:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"total={len(df)} valid={len(valid)} rejected={len(rejected)}")
    print(Counter(e.split(":")[0] for r in rejected for e in r["errors"]))

    if rejected and args.fail_fast:                    # hentikan SEBELUM memuat
        print("FAIL-FAST: batch dihentikan, tidak ada data dimuat.")
        sys.exit(1)
    pd.DataFrame(valid).to_parquet("data/valid/valid_orders.parquet", index=False)

if __name__ == "__main__":
    main()