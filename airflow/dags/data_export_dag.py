"""
data_export_dag.py
==================
Export predictions from MongoDB to a local backup file.
Runs weekly on Sundays at 4:00 AM.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os
import json
from pymongo import MongoClient


@dag(
    dag_id="data_export",
    description="Export MongoDB predictions to backup file",
    schedule="0 4 * * 0",                      # Every Sunday at 04:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["mongodb", "backup", "export"],
)
def data_export():
    """Export predictions from MongoDB to local backup"""

    @task()
    def connect_mongodb():
        """Connect to MongoDB and get stats"""
        host     = os.getenv("MONGODB_HOST",          "mongodb")
        port     = int(os.getenv("MONGODB_PORT",      "27017"))
        user     = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name  = os.getenv("MONGODB_DATABASE",      "amazon_reviews")

        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        client = MongoClient(uri)
        db = client[db_name]
        
        # Count documents in each collection
        predictions_count = db["predictions"].count_documents({})
        agg_count = db["agg_monthly_sentiments"].count_documents({})
        
        print(f"📊 MongoDB Stats:")
        print(f"   Predictions: {predictions_count} records")
        print(f"   Aggregations: {agg_count} records")
        
        client.close()
        return {
            "predictions": predictions_count,
            "aggregations": agg_count,
            "timestamp": datetime.now().isoformat()
        }

    @task()
    def export_predictions():
        """Export latest predictions to backup file"""
        host     = os.getenv("MONGODB_HOST",          "mongodb")
        port     = int(os.getenv("MONGODB_PORT",      "27017"))
        user     = os.getenv("MONGODB_ROOT_USER",     "admin")
        password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
        db_name  = os.getenv("MONGODB_DATABASE",      "amazon_reviews")

        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        client = MongoClient(uri)
        db = client[db_name]
        
        # Get recent predictions (last 100)
        predictions = list(db["predictions"].find().sort("_id", -1).limit(100))
        
        # Convert ObjectId to string for JSON serialization
        for pred in predictions:
            if "_id" in pred:
                pred["_id"] = str(pred["_id"])
        
        # Save to file
        backup_file = "/opt/airflow/backups/predictions_backup.json"
        os.makedirs(os.path.dirname(backup_file), exist_ok=True)
        
        with open(backup_file, "w") as f:
            json.dump(predictions, f, indent=2)
        
        print(f"✅ Exported {len(predictions)} predictions to {backup_file}")
        
        client.close()
        return {"exported": len(predictions), "file": backup_file}

    @task()
    def send_summary(mongo_stats: dict, export_stats: dict):
        """Final summary task"""
        print("=" * 50)
        print("  DATA EXPORT SUMMARY")
        print("=" * 50)
        print(f"  Total Records: {mongo_stats['predictions']}")
        print(f"  Exported: {export_stats['exported']}")
        print(f"  Timestamp: {mongo_stats['timestamp']}")
        print(f"  File: {export_stats['file']}")
        print("=" * 50)

    # Define task flow
    stats = connect_mongodb()
    export_result = export_predictions()
    send_summary(stats, export_result)


# Instantiate the DAG
dag = data_export()
