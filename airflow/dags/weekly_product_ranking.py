"""
weekly_product_ranking.py
=========================
Runs weekly on Monday at 09:00 UTC (after model_evaluation_dag at 08:00).

This DAG implements the recommendation logic from DAGS_AND_RECO.md:
1. Read aggregated product sentiment scores from agg_product_scoring.
2. Apply Confidence Filter: total reviews >= 3.
3. Calculate Satisfaction Score: (positive / total) * 100.
4. Extract Top 10 Recommended (Pépites) and Top 10 Flops (Quality Alerts).

Persists the results to MongoDB collection: product_rankings
"""

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

CONFIDENCE_THRESHOLD = 3   # minimum reviews to be eligible for ranking


def _mongo_client():
    from pymongo import MongoClient
    host     = os.getenv("MONGODB_HOST",          "mongodb")
    port     = int(os.getenv("MONGODB_PORT",      "27017"))
    user     = os.getenv("MONGODB_ROOT_USER",     "admin")
    password = os.getenv("MONGODB_ROOT_PASSWORD", "admin123")
    return MongoClient(
        f"mongodb://{user}:{password}@{host}:{port}/",
        serverSelectionTimeoutMS=5000,
    )


@dag(
    dag_id="weekly_product_ranking",
    description="Calculates Top 10 Recommended and Top 10 Flops based on AI sentiment",
    schedule="0 9 * * 1",          # every Monday at 09:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["recommendation", "ranking", "weekly"],
)
def weekly_product_ranking():

    @task()
    def calculate_rankings() -> dict:
        client  = _mongo_client()
        db_name = os.getenv("MONGODB_DATABASE", "amazon_reviews")
        agg_col = os.getenv("MONGO_COLLECTION_PRODUCT_SCORING", "agg_product_scoring")
        db      = client[db_name]

        # 1. Fetch all aggregated product data
        # Note: agg_product_scoring is grouped by _id = ProductId
        cursor = db[agg_col].find({})
        
        products = []
        for doc in cursor:
            # Structure expected from aggregate_dashboard_dag:
            # { "_id": "B00...", "total": 10, "sentiments": [{"sentiment": "positive", "count": 8}, ...] }
            product_id = doc["_id"]
            total      = doc.get("total", 0)
            
            if total < CONFIDENCE_THRESHOLD:
                continue
                
            sentiments = doc.get("sentiments", [])
            pos_count = 0
            neg_count = 0
            for s in sentiments:
                label = (s.get("sentiment") or "").lower()
                if label == "positive":
                    pos_count = s.get("count", 0)
                elif label == "negative":
                    neg_count = s.get("count", 0)

            satisfaction_rate = round((pos_count / total) * 100, 1) if total > 0 else 0
            dissatisfaction_rate = round((neg_count / total) * 100, 1) if total > 0 else 0
            
            products.append({
                "product_id": product_id,
                "total_reviews": total,
                "positive_count": pos_count,
                "negative_count": neg_count,
                "satisfaction_rate": satisfaction_rate,
                "dissatisfaction_rate": dissatisfaction_rate
            })

        # 2. Sort for Pépites (High Satisfaction)
        top_recommended = sorted(products, key=lambda x: x["satisfaction_rate"], reverse=True)[:10]
        
        # 3. Sort for Flops (High Dissatisfaction)
        # Note: We sort by dissatisfaction rate to find the most "disappointing" products
        quality_alerts = sorted(products, key=lambda x: x["dissatisfaction_rate"], reverse=True)[:10]

        client.close()
        
        return {
            "top_recommended": top_recommended,
            "quality_alerts":  quality_alerts,
            "total_eligible_products": len(products)
        }

    @task()
    def save_rankings(rankings: dict):
        client     = _mongo_client()
        db_name    = os.getenv("MONGODB_DATABASE", "amazon_reviews")
        rankings_col = os.getenv("MONGO_COLLECTION_RECOMMENDATIONS", "product_rankings")
        db         = client[db_name]

        doc = {
            "updated_at":      datetime.utcnow().isoformat(),
            "week_label":      datetime.utcnow().strftime("%Y-W%U"),
            "top_recommended": rankings["top_recommended"],
            "quality_alerts":  rankings["quality_alerts"],
            "metadata": {
                "confidence_threshold": CONFIDENCE_THRESHOLD,
                "total_eligible":       rankings["total_eligible_products"]
            }
        }

        # Keep only the latest ranking (or you could keep history, but for simplicity we replace)
        db[rankings_col].replace_one({"type": "weekly_rankings"}, {**doc, "type": "weekly_rankings"}, upsert=True)
        client.close()
        
        print(f"✅ Success: Top 10 Pépites and Top 10 Flops saved for {doc['week_label']}")

    rank_data = calculate_rankings()
    save_rankings(rank_data)


dag_instance = weekly_product_ranking()
