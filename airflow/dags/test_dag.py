"""
test_connections_dag.py
=======================
Test DAG — verifies Airflow can reach Kafka and MongoDB.
Run this manually once to confirm everything is wired correctly.
"""

from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="test_connections",
    description="Test connectivity to Kafka and MongoDB",
    schedule=None,          # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "connectivity"],
)
def test_connections():

    @task()
    def test_kafka():
        import os
        from kafka import KafkaAdminClient
        from kafka.errors import NoBrokersAvailable

        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="airflow-test",
                request_timeout_ms=5000,
            )
            topics = admin.list_topics()
            admin.close()
            print(f"✅ Kafka connected at {bootstrap}")
            print(f"   Topics found: {topics}")
            return {"status": "ok", "topics": topics}
        except NoBrokersAvailable:
            raise Exception(f"❌ Cannot reach Kafka at {bootstrap}")

    @task()
    def test_mongodb():
        import os
        from pymongo import MongoClient
        from pymongo.errors import ServerSelectionTimeoutError

        host     = os.getenv("MONGODB_HOST",          "mongodb")
        port     = int(os.getenv("MONGODB_PORT",      "27017"))
        user     = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name  = os.getenv("MONGODB_DATABASE",      "amazon_reviews")

        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.server_info()   # forces connection
            db = client[db_name]
            collections = db.list_collection_names()
            count = db["predictions"].count_documents({})
            client.close()
            print(f"✅ MongoDB connected at {host}:{port}")
            print(f"   Database    : {db_name}")
            print(f"   Collections : {collections}")
            print(f"   Predictions : {count} documents")
            return {"status": "ok", "collections": collections, "predictions": count}
        except ServerSelectionTimeoutError:
            raise Exception(f"❌ Cannot reach MongoDB at {host}:{port}")

    @task()
    def summarize(kafka_result: dict, mongo_result: dict):
        print("=" * 45)
        print("  CONNECTION TEST SUMMARY")
        print("=" * 45)
        print(f"  Kafka   : {kafka_result['status'].upper()}")
        print(f"  MongoDB : {mongo_result['status'].upper()}")
        print(f"  Predictions in DB: {mongo_result['predictions']}")
        print("=" * 45)

    kafka_result = test_kafka()
    mongo_result = test_mongodb()
    summarize(kafka_result, mongo_result)


dag_instance = test_connections()