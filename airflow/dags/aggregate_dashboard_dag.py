"""
aggregation_dag.py
==================
Runs nightly — pre-computes monthly sentiment stats and
per-product scoring into a dedicated MongoDB collection.
FastAPI dashboard reads from these pre-computed collections
instead of running expensive aggregations on every request.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="dashboard_aggregation",
    description="Pre-compute dashboard stats from predictions",
    schedule="0 2 * * *",       # every day at 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mongodb", "dashboard", "aggregation"],
)
def dashboard_aggregation():

    @task()
    def aggregate_monthly_sentiments():
        """
        Groups predictions by month and sentiment.
        Writes result to the monthly aggregation collection.
        """
        import os
        from pymongo import MongoClient

        host = os.getenv("MONGODB_HOST",          "mongodb")
        port = int(os.getenv("MONGODB_PORT",      "27017"))
        user = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name = os.getenv("MONGODB_DATABASE",      "amazon_reviews")

        agg_monthly = os.getenv(
            "MONGO_COLLECTION_MONTHLY", "agg_monthly_sentiments"
        )

        client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/",
            serverSelectionTimeoutMS=5000,
        )
        db = client[db_name]

        pipeline = [
            {"$match": {"PredictedSentiment": {
                "$exists": True}, "Time": {"$exists": True}}},
            {"$addFields": {
                "ts": {
                    "$convert": {
                        "input": "$Time",
                        "to": "long",
                        "onError": None,
                        "onNull": None,
                    }
                }
            }},
            {"$match": {"ts": {"$ne": None}}},
            {"$addFields": {
                "date": {"$toDate": {"$multiply": ["$ts", 1000]}}
            }},
            {"$group": {
                "_id": {
                    "year":  {"$year":  "$date"},
                    "month": {"$month": "$date"},
                    "sentiment": "$PredictedSentiment",
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.year": 1, "_id.month": 1}},
        ]

        results = list(db["predictions"].aggregate(pipeline))
        aggregated_at = datetime.utcnow()
        for doc in results:
            doc["aggregated_at"] = aggregated_at

        # Replace the aggregation collection entirely
        db[agg_monthly].drop()
        if results:
            db[agg_monthly].insert_many(results)

        print(f"✅ Monthly aggregation done — {len(results)} buckets written")
        return len(results)

    @task()
    def aggregate_product_scoring():
        """
        Computes sentiment breakdown per ProductId.
        Writes result to the product scoring aggregation collection.
        """
        import os
        from pymongo import MongoClient

        host = os.getenv("MONGODB_HOST",          "mongodb")
        port = int(os.getenv("MONGODB_PORT",      "27017"))
        user = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name = os.getenv("MONGODB_DATABASE",      "amazon_reviews")

        agg_product = os.getenv(
            "MONGO_COLLECTION_PRODUCT_SCORING", "agg_product_scoring"
        )

        client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/",
            serverSelectionTimeoutMS=5000,
        )
        db = client[db_name]

        pipeline = [
            {"$match": {"PredictedSentiment": {"$exists": True},
                        "ProductId": {"$exists": True}}},
            {"$group": {
                "_id": {
                    "product":   "$ProductId",
                    "sentiment": "$PredictedSentiment",
                },
                "count": {"$sum": 1}
            }},
            {"$group": {
                "_id": "$_id.product",
                "sentiments": {
                    "$push": {
                        "sentiment": "$_id.sentiment",
                        "count":     "$count"
                    }
                },
                "total": {"$sum": "$count"}
            }},
            {"$sort": {"total": -1}},
        ]

        results = list(db["predictions"].aggregate(pipeline))
        aggregated_at = datetime.utcnow()
        for doc in results:
            doc["aggregated_at"] = aggregated_at

        db[agg_product].drop()
        if results:
            db[agg_product].insert_many(results)

        print(f"✅ Product scoring aggregation done — {len(results)} products")
        return len(results)

    @task()
    def report(monthly_buckets: int, product_count: int):
        print("=" * 50)
        print("  NIGHTLY AGGREGATION COMPLETE")
        print("=" * 50)
        print(f"  Monthly buckets : {monthly_buckets}")
        print(f"  Products scored : {product_count}")
        print(f"  Timestamp       : {datetime.utcnow().isoformat()}")
        print("=" * 50)

    monthly = aggregate_monthly_sentiments()
    products = aggregate_product_scoring()
    report(monthly, products)


dag_instance = dashboard_aggregation()
