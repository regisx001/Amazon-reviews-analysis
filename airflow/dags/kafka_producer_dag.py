"""
producer_dag.py
===============
Sends a batch of Amazon reviews to Kafka every hour.
Uses the test CSV already in /opt/airflow/data/test/
"""

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="amazon_reviews_producer",
    description="Send review batches to Kafka on schedule",
    schedule="0 * * * *",       # every hour at :00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["kafka", "producer"],
)
def amazon_reviews_producer():

    @task()
    def check_kafka_ready():
        from kafka import KafkaAdminClient
        from kafka.errors import NoBrokersAvailable
        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="airflow-health-check",
                request_timeout_ms=5000,
            )
            topics = admin.list_topics()
            admin.close()
            print(f"✅ Kafka ready — topics: {topics}")
            return True
        except NoBrokersAvailable:
            raise Exception(f"❌ Kafka not reachable at {bootstrap}")

    @task()
    def send_batch(kafka_ready: bool):
        import glob
        import json
        import time
        import csv
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
        topic = os.getenv("KAFKA_TOPIC",     "reviews.raw")
        data_dir = os.getenv("DATA_DIR",        "/opt/airflow/data")
        batch_size = int(os.getenv("PRODUCER_BATCH_SIZE", "50"))
        send_delay = float(os.getenv("PRODUCER_DELAY",    "0.1"))

        # Find test CSV files
        pattern = os.path.join(data_dir, "test", "*.csv")
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"No CSV files found at {pattern}")

        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )

        sent = 0
        for csv_file in files:
            with open(csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if sent >= batch_size:
                        break
                    producer.send(topic, value=dict(row))
                    sent += 1
                    time.sleep(send_delay)
            if sent >= batch_size:
                break

        producer.flush()
        producer.close()
        print(f"✅ Sent {sent} reviews to topic '{topic}'")
        return sent

    @task()
    def log_run_stats(sent_count: int):
        from pymongo import MongoClient
        host = os.getenv("MONGODB_HOST",          "mongodb")
        port = int(os.getenv("MONGODB_PORT",      "27017"))
        user = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name = os.getenv("MONGODB_DATABASE",      "amazon_reviews")

        client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/",
            serverSelectionTimeoutMS=5000,
        )
        total_predictions = client[db_name]["predictions"].count_documents({})
        client.close()

        print("=" * 50)
        print(f"  Reviews sent this run : {sent_count}")
        print(f"  Total in MongoDB      : {total_predictions}")
        print("=" * 50)

    ready = check_kafka_ready()
    sent = send_batch(ready)
    log_run_stats(sent)


dag_instance = amazon_reviews_producer()
