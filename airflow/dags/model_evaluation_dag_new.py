"""
model_evaluation_dag.py
=======================
Runs weekly — checks if the model's prediction distribution
is drifting (e.g. neutral ratio spiking = model degradation).
Logs a warning if thresholds are exceeded.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


NEUTRAL_DRIFT_THRESHOLD = 0.30   # alert if neutral > 30%
NEGATIVE_DRIFT_THRESHOLD = 0.40   # alert if negative > 40%
MIN_PREDICTIONS = 100    # skip check if not enough data yet


@dag(
    dag_id="model_evaluation",
    description="Weekly model prediction distribution check",
    schedule="0 8 * * 1",      # every Monday at 08:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ml", "evaluation", "monitoring"],
)
def model_evaluation():

    @task()
    def compute_distribution():
        import os
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
        db = client[db_name]

        pipeline = [
            {"$group": {"_id": "$PredictedSentiment", "count": {"$sum": 1}}}
        ]
        results = list(db["predictions"].aggregate(pipeline))
        client.close()

        counts = {"positive": 0, "neutral": 0, "negative": 0}
        for r in results:
            s = (r["_id"] or "").lower()
            if s in counts:
                counts[s] = r["count"]

        total = sum(counts.values())
        print(f"  Total predictions : {total}")
        print(f"  Positive          : {counts['positive']}")
        print(f"  Neutral           : {counts['neutral']}")
        print(f"  Negative          : {counts['negative']}")
        return {"counts": counts, "total": total}

    @task()
    def check_drift(distribution: dict):
        counts = distribution["counts"]
        total = distribution["total"]

        if total < MIN_PREDICTIONS:
            print(f"⚠️  Only {total} predictions — skipping drift check")
            return {
                "drift_detected": False,
                "reason": "insufficient_data",
                "neutral_ratio": 0.0,
                "negative_ratio": 0.0,
                "alerts": [],
            }

        neutral_ratio = counts["neutral"] / total
        negative_ratio = counts["negative"] / total

        print(f"  Neutral  ratio : {neutral_ratio:.2%}")
        print(f"  Negative ratio : {negative_ratio:.2%}")

        alerts = []
        if neutral_ratio > NEUTRAL_DRIFT_THRESHOLD:
            alerts.append(
                f"NEUTRAL drift: {neutral_ratio:.2%} > {NEUTRAL_DRIFT_THRESHOLD:.0%} threshold"
            )
        if negative_ratio > NEGATIVE_DRIFT_THRESHOLD:
            alerts.append(
                f"NEGATIVE drift: {negative_ratio:.2%} > {NEGATIVE_DRIFT_THRESHOLD:.0%} threshold"
            )

        if alerts:
            for a in alerts:
                print(f"🚨 ALERT — {a}")
            return {
                "drift_detected": True,
                "alerts": alerts,
                "neutral_ratio": neutral_ratio,
                "negative_ratio": negative_ratio,
            }

        print("✅ No drift detected — model distribution is healthy")
        return {
            "drift_detected": False,
            "alerts": [],
            "neutral_ratio": neutral_ratio,
            "negative_ratio": negative_ratio,
        }

    @task()
    def persist_drift_status(distribution: dict, drift_result: dict):
        import os
        from pymongo import MongoClient

        host = os.getenv("MONGODB_HOST",          "mongodb")
        port = int(os.getenv("MONGODB_PORT",      "27017"))
        user = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name = os.getenv("MONGODB_DATABASE",      "amazon_reviews")
        drift_collection = os.getenv(
            "MONGO_COLLECTION_DRIFT", "model_drift_status")

        client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/",
            serverSelectionTimeoutMS=5000,
        )
        db = client[db_name]

        counts = distribution.get("counts") or {}
        doc = {
            "evaluated_at": datetime.utcnow(),
            "total": distribution.get("total", 0),
            "counts": {
                "positive": counts.get("positive", 0),
                "neutral": counts.get("neutral", 0),
                "negative": counts.get("negative", 0),
            },
            "neutral_ratio": drift_result.get("neutral_ratio", 0.0),
            "negative_ratio": drift_result.get("negative_ratio", 0.0),
            "drift_detected": drift_result.get("drift_detected", False),
            "alerts": drift_result.get("alerts", []),
            "reason": drift_result.get("reason"),
        }

        db[drift_collection].insert_one(doc)
        client.close()
        print("Drift status stored in MongoDB")

    @task()
    def persist_model_insights():
        import os
        import json
        from pathlib import Path
        from pymongo import MongoClient

        host = os.getenv("MONGODB_HOST",          "mongodb")
        port = int(os.getenv("MONGODB_PORT",      "27017"))
        user = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name = os.getenv("MONGODB_DATABASE",      "amazon_reviews")
        insights_collection = os.getenv(
            "MONGO_COLLECTION_MODEL_INSIGHTS", "model_insights"
        )

        metrics_path = os.getenv(
            "MODEL_INSIGHTS_PATH", "/opt/airflow/data/model_insights.json"
        )

        errors = []
        payload = None
        metrics_file = Path(metrics_path)
        if metrics_file.exists():
            try:
                payload = json.loads(metrics_file.read_text(encoding="utf-8"))
            except Exception as exc:
                errors.append(f"invalid_json: {exc}")
        else:
            errors.append("missing_file")

        status = "ok" if payload else "missing"
        evaluated_at = (payload or {}).get("evaluated_at") or datetime.utcnow().isoformat()
        metadata = (payload or {}).get("metadata") or {
            "model_name": (payload or {}).get("model_name"),
            "model_version": (payload or {}).get("model_version"),
            "trained_at": (payload or {}).get("trained_at"),
            "dataset": (payload or {}).get("dataset"),
            "notes": (payload or {}).get("notes"),
            "parameters": (payload or {}).get("parameters"),
        }

        doc = {
            "evaluated_at": evaluated_at,
            "status": status,
            "metrics": (payload or {}).get("metrics"),
            "confusion_matrix": (payload or {}).get("confusion_matrix"),
            "metadata": metadata,
            "source": {"path": str(metrics_file)},
            "errors": errors,
        }

        client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/",
            serverSelectionTimeoutMS=5000,
        )
        db = client[db_name]
        db[insights_collection].insert_one(doc)
        client.close()
        print("Model insights stored in MongoDB")

    @task()
    def final_report(distribution: dict, drift_result: dict):
        total = distribution["total"]
        counts = distribution["counts"]
        print("=" * 50)
        print("  WEEKLY MODEL EVALUATION REPORT")
        print("=" * 50)
        print(
            f"  Evaluated at : {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  Total predictions : {total}")
        if total > 0:
            for s, n in counts.items():
                print(f"  {s.capitalize():10s} : {n:>6} ({n/total:.1%})")
        print("-" * 50)
        if drift_result["drift_detected"]:
            print("  STATUS  : ⚠️  DRIFT DETECTED")
            for alert in drift_result["alerts"]:
                print(f"  → {alert}")
        else:
            print("  STATUS  : ✅ HEALTHY")
        print("=" * 50)

    distribution = compute_distribution()
    drift_result = check_drift(distribution)
    drift_task = persist_drift_status(distribution, drift_result)

    run_spark_evaluation = SparkSubmitOperator(
        task_id="run_spark_evaluation",
        conn_id="spark_default",
        application="/opt/airflow/spark/evaluation_job/spark_evaluation_job.py",
        name="airflow-spark-evaluation",
        verbose=True,
    )

    insights_task = persist_model_insights()

    # The spark job evaluates the model and writes insights json, so it MUST
    # run before persisting insights to MongoDB.
    run_spark_evaluation >> insights_task

    final_report(distribution, drift_result)


dag_instance = model_evaluation()
