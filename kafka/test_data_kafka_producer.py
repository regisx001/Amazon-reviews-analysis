#!/usr/bin/env python3
"""
test_data_kafka_producer.py — Stream CSV rows to Kafka
======================================================
Streams rows from a CSV file (or glob) to the reviews.raw topic with a
delay between messages to simulate real-time data.

Usage:
	python kafka/test_data_kafka_producer.py
	python kafka/test_data_kafka_producer.py --bootstrap localhost:29092 --csv data/output/test/part-*.csv
"""

import argparse
import csv
import glob
import json
import time
import os

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "reviews.raw")
TEST_CSV = os.getenv("TEST_CSV", "data/output/test/part-*.csv")
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "0"))
SEND_DELAY = float(os.getenv("KAFKA_SEND_DELAY", "0.8"))


def score_to_sentiment(score) -> str:
    try:
        s = float(score)
    except (TypeError, ValueError):
        return "unknown"
    if s <= 2:
        return "negative"
    if s == 3:
        return "neutral"
    return "positive"


def _value(row, keymap, key):
    src = keymap.get(key.lower())
    return row.get(src) if src else None


def read_csv_messages(csv_path: str, limit: int):
    messages = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return messages
        keymap = {name.lower(): name for name in reader.fieldnames}
        count = 0
        for row in reader:
            msg = {
                "Id": _value(row, keymap, "Id"),
                "ProductId": _value(row, keymap, "ProductId"),
                "UserId": _value(row, keymap, "UserId"),
                "ProfileName": _value(row, keymap, "ProfileName"),
                "HelpfulnessNumerator": _value(row, keymap, "HelpfulnessNumerator"),
                "HelpfulnessDenominator": _value(row, keymap, "HelpfulnessDenominator"),
                "Score": _value(row, keymap, "Score"),
                "Time": _value(row, keymap, "Time"),
                "Summary": _value(row, keymap, "Summary"),
                "Text": _value(row, keymap, "Text"),
            }
            count += 1
            if not msg["Id"]:
                msg["Id"] = f"TEST_{count:03d}"
            if not msg["Time"]:
                msg["Time"] = str(int(time.time()))
            messages.append(msg)
            if limit > 0 and count >= limit:
                break
    return messages


def build_producer(bootstrap: str, retries: int = 10) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
            )
            print(f"Connected to Kafka at {bootstrap}")
            return producer
        except NoBrokersAvailable:
            print(
                f"Kafka not ready (attempt {attempt}/{retries}), retrying in 2s...")
            time.sleep(2)
    raise NoBrokersAvailable(f"Could not connect after {retries} attempts.")


def parse_args():
    p = argparse.ArgumentParser(description="Send CSV rows to Kafka.")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=KAFKA_TOPIC)
    p.add_argument("--csv", default=TEST_CSV)
    p.add_argument("--limit", type=int, default=TEST_LIMIT)
    p.add_argument("--delay", type=float, default=SEND_DELAY)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    topic = args.topic
    csv_path = args.csv
    limit = args.limit
    delay = max(0.0, args.delay)
    paths = sorted(glob.glob(csv_path))
    if not paths:
        print(f"No CSV files matched: {csv_path}")
        raise SystemExit(1)

    print("=" * 55)
    print("  Kafka Test Producer — CSV Reviews")
    print("=" * 55)
    print(f"  Bootstrap : {args.bootstrap}")
    print(f"  Topic     : {topic}")
    print(f"  CSV       : {csv_path}")
    print(f"  Files     : {len(paths)}")
    print(f"  Limit     : {limit} (0 = no limit)")
    print(f"  Delay     : {delay}s")
    print("=" * 55 + "\n")

    producer = build_producer(args.bootstrap)

    message_count = 0
    for csv_file in paths:
        print(f"Streaming file: {csv_file}")
        messages = read_csv_messages(csv_file, limit)
        if not messages:
            print(f"No rows found in {csv_file}, skipping.")
            continue
        for msg in messages:
            expected = score_to_sentiment(msg.get("Score"))

            future = producer.send(topic, value=msg)
            future.get(timeout=10)

            print(f"  Sent [{expected.upper():8s}] "
                  f"Id={msg['Id']}  Score={msg.get('Score')}  "
                  f"Text={str(msg.get('Text', ''))[:55]}...")
            message_count += 1
            time.sleep(delay)

    producer.flush()
    producer.close()

    print("\n" + "=" * 55)
    print(f"  {message_count} messages delivered to Kafka.")
    print("=" * 55)
