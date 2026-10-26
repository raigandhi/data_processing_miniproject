import argparse, json
from pathlib import Path
from kafka import KafkaConsumer
from pydantic import ValidationError
from contract import Order

ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="orders")
ap.add_argument("--group", default="dpw-orders")
args = ap.parse_args()

VALID = Path("data/stream/valid_events.jsonl")
REJECT = Path("data/stream/rejected_events.jsonl")
VALID.parent.mkdir(parents=True, exist_ok=True)

def append(path, obj):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# Idempotensi sink: ingat order_id yang sudah tersimpan (tahan restart)
seen = set()
if VALID.exists():
    seen = {json.loads(l)["order_id"] for l in open(VALID, encoding="utf-8")}

consumer = KafkaConsumer(
    args.topic, bootstrap_servers="localhost:9092", group_id=args.group,
    auto_offset_reset="earliest", enable_auto_commit=False,   # commit manual
    consumer_timeout_ms=10000,                                # berhenti bila sepi 10 detik
)
stats = {"valid": 0, "rejected": 0, "duplicate": 0}
for msg in consumer:
    meta = {"topic": msg.topic, "partition": msg.partition, "offset": msg.offset}
    try:
        payload = json.loads(msg.value.decode("utf-8"))
        order = Order.model_validate(payload).model_dump(mode="json")
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        append(REJECT, {"meta": meta, "raw": msg.value.decode("utf-8", "replace"),
                        "errors": [f"json: {e}"]}); stats["rejected"] += 1
    except ValidationError as e:
        append(REJECT, {"meta": meta, "payload": payload,
                        "errors": [f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors()]})
        stats["rejected"] += 1
    else:
        if order["order_id"] in seen:
            stats["duplicate"] += 1
            print(f"DUPLIKAT dilewati: {order['order_id']} {meta}")
        else:
            append(VALID, order); seen.add(order["order_id"]); stats["valid"] += 1
    consumer.commit()               # commit SETELAH sink selesai
print(stats)
consumer.close()