"""Stream validated CSV rows to Kafka, preserving amounts exactly as decimals."""
import argparse
import csv
import hashlib
import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path


def messages(path):
    path = Path(path)
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"transaction_time", "us_state", "cat_id", "amount"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"{path}: missing columns {required - set(reader.fieldnames or [])}")
        for number, row in enumerate(reader, 2):
            try:
                amount = Decimal(row["amount"])
                if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")) or abs(amount) >= 10**16:
                    raise ValueError("amount must fit Decimal(18,2)")
                if not row["us_state"] or not row["cat_id"]:
                    raise ValueError("state/category is empty")
                timestamp = datetime.fromisoformat(row["transaction_time"])
                if timestamp.tzinfo is not None or not 1970 <= timestamp.year <= 2105:
                    raise ValueError("expected timezone-free timestamp in DateTime range")
                raw = json.dumps(row, ensure_ascii=False, sort_keys=True)
                identifier = hashlib.sha256(f"{path.name}:{number}:{raw}".encode()).hexdigest()
                yield {"transaction_id": identifier, "source_file": path.name,
                       "transaction_time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                       "us_state": row["us_state"], "cat_id": row["cat_id"],
                       "amount": str(amount), "raw_json": raw}
            except (ValueError, ArithmeticError, TypeError) as error:
                raise ValueError(f"{path}:{number}: {error}") from error


def produce(paths, topic="hw2_transactions"):
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                         "enable.idempotence": True, "acks": "all", "linger.ms": 20,
                         "compression.type": "gzip", "delivery.timeout.ms": 120000})
    errors = []
    def delivered(error, message):
        if error:
            errors.append(str(error))
    count = 0
    try:
        for path in paths:
            for row in messages(path):
                payload = json.dumps(row, ensure_ascii=False).encode()
                while True:
                    try:
                        producer.produce(topic, key=row["transaction_id"], value=payload, on_delivery=delivered)
                        break
                    except BufferError:
                        producer.poll(0.1)
                producer.poll(0)
                if errors:
                    raise RuntimeError(errors[0])
                count += 1
                if count % 100000 == 0:
                    print(f"Queued {count:,} rows", flush=True)
    finally:
        remaining = producer.flush(120)
    if remaining or errors:
        raise RuntimeError(f"Kafka delivery failed: remaining={remaining}; {errors[:3]}")
    print(f"Kafka acknowledged {count:,} rows", flush=True)
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", nargs="+")
    parser.add_argument("--topic", default="hw2_transactions")
    args = parser.parse_args()
    produce(args.csv, args.topic)
