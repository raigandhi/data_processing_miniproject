import argparse, csv, json
from kafka import KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--source", default="data/orders.csv")
ap.add_argument("--topic", default="orders")
ap.add_argument("--limit", type=int, default=36)
ap.add_argument("--duplicate", help="order_id yang dikirim dua kali")
ap.add_argument("--bad-json", action="store_true")
args = ap.parse_args()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
    acks="all",
)
rows = list(csv.DictReader(open(args.source, encoding="utf-8")))[: args.limit]
for row in rows:
    meta = producer.send(args.topic, key=row["order_id"], value=row).get(timeout=10)
    print(f"sent {row['order_id']} -> partition={meta.partition} offset={meta.offset}")
    if row["order_id"] == args.duplicate:              # demo duplikasi
        producer.send(args.topic, key=row["order_id"], value=row).get(timeout=10)
        print(f"sent DUPLICATE {row['order_id']}")
if args.bad_json:                                      # demo JSON rusak
    producer.send(args.topic, key="BAD", value=b"{not json").get(timeout=10)
producer.flush()