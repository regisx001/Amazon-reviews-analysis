#!/usr/bin/env python3
"""
test_streaming.py — Stream first N rows from CSV to Kafka
=========================================================
Sends the first N rows from a CSV file to the reviews.raw topic and
optionally prints predictions from MongoDB.

Usage:
        python test_streaming.py
        python test_streaming.py --bootstrap localhost:29092 --csv data/reviews.csv
"""

import argparse
import csv
import json
import time
import os

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import pymongo

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC",     "reviews.raw")
TEST_CSV = os.getenv("TEST_CSV", "data/reviews.csv")
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "10"))

MONGO_DB = os.getenv("MONGODB_DATABASE", "amazon_reviews")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "predictions")
MONGO_URI = os.getenv("MONGO_URI") or (
    "mongodb://{user}:{pw}@{host}:{port}/".format(
        user=os.getenv("MONGODB_ROOT_USER", "admin"),
        pw=os.getenv("MONGODB_ROOT_PASSWORD", "admin123"),
        host=os.getenv("MONGODB_HOST", "mongodb"),
        port=os.getenv("MONGODB_PORT", "27017"),
    )
)
MONGO_WAIT_SECONDS = int(os.getenv("MONGO_WAIT_SECONDS", "15"))


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
            if count >= limit:
                break
    return messages


def fetch_predictions(ids, mongo_uri, mongo_db, mongo_collection, wait_seconds):
    ids = list(ids)
    if not ids:
        return {}
    deadline = time.time() + max(0, wait_seconds)
    found = {}
    client = pymongo.MongoClient(mongo_uri)
    try:
        coll = client[mongo_db][mongo_collection]
        while time.time() < deadline and len(found) < len(ids):
            for doc in coll.find({"Id": {"$in": ids}}):
                doc_id = doc.get("Id")
                if doc_id:
                    found[doc_id] = doc
            if len(found) < len(ids):
                time.sleep(1.0)
    finally:
        client.close()
    return found


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
            print(f"✅ Connected to Kafka at {bootstrap}")
            return producer
        except NoBrokersAvailable:
            print(
                f"⏳ Kafka not ready (attempt {attempt}/{retries}), retrying in 2s...")
            time.sleep(2)
    raise NoBrokersAvailable(f"Could not connect after {retries} attempts.")


def parse_args():
    p = argparse.ArgumentParser(description="Send CSV rows to Kafka.")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--topic",     default=KAFKA_TOPIC)
    p.add_argument("--csv",       default=TEST_CSV)
    p.add_argument("--limit",     type=int, default=TEST_LIMIT)
    p.add_argument("--wait-seconds", type=int, default=MONGO_WAIT_SECONDS)
    p.add_argument("--mongo-uri", default=MONGO_URI)
    p.add_argument("--mongo-db", default=MONGO_DB)
    p.add_argument("--mongo-collection", default=MONGO_COLLECTION)
    p.add_argument("--skip-mongo", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    topic = args.topic
    csv_path = args.csv
    limit = args.limit

    print("=" * 55)
    print("  Kafka Test Producer — CSV Reviews")
    print("=" * 55)
    print(f"  Bootstrap : {args.bootstrap}")
    print(f"  Topic     : {topic}")
    print(f"  CSV       : {csv_path}")
    print(f"  Limit     : {limit}")
    print("=" * 55 + "\n")

    messages = read_csv_messages(csv_path, limit)
    if not messages:
        print("No rows found to send. Check the CSV path and content.")
        raise SystemExit(1)

    producer = build_producer(args.bootstrap)

    expected_map = {}

    for msg in messages:
        expected = score_to_sentiment(msg.get("Score"))
        expected_map[msg["Id"]] = expected

        future = producer.send(topic, value=msg)
        future.get(timeout=10)                      # block until broker ACK

        print(f"  ✅ Sent [{expected.upper():8s}] "
              f"Id={msg['Id']}  Score={msg.get('Score')}  "
              f"Text={str(msg.get('Text', ''))[:55]}...")
        time.sleep(0.5)

    producer.flush()
    producer.close()

    print("\n" + "=" * 55)
    print(f"  {len(messages)} messages delivered to Kafka.")
    print("=" * 55)

    if args.skip_mongo:
        print("\nSkipping MongoDB lookup.")
    else:
        print("\nWaiting for predictions in MongoDB...")
        ids = [m["Id"] for m in messages]
        predictions = fetch_predictions(
            ids,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection,
            wait_seconds=args.wait_seconds,
        )

        print("\nPredictions from MongoDB:")
        for msg_id in ids:
            expected = expected_map.get(msg_id, "unknown")
            doc = predictions.get(msg_id)
            if not doc:
                print(f"  Id={msg_id} expected={expected} predicted=not_found")
                continue
            predicted = doc.get("PredictedSentiment", "unknown")
            raw_pred = doc.get("prediction", "n/a")
            print(
                f"  Id={msg_id} expected={expected} "
                f"predicted={predicted} prediction={raw_pred}"
            )
