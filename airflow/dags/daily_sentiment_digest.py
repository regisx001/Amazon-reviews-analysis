"""
daily_sentiment_digest.py
=========================
Runs daily at 06:00 UTC (after nightly aggregation DAG at 02:00).

Builds a full day journal from the last 24h of predictions:
  - Global sentiment counts and percentages
  - Day-over-day delta vs yesterday
  - Health score 0-100
  - Top 3 most-reviewed products
  - Peak activity hour

Writes one document per day to MongoDB: daily_digest
"""

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task


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


def _sentiment_counts(db, since_str: str, until_str: str) -> dict:
    """Aggregate sentiment counts for a given time window."""
    pipeline = [
        {"$match": {
            "ProcessingTime": {"$gte": since_str, "$lt": until_str},
            "PredictedSentiment": {"$exists": True},
        }},
        {"$group": {"_id": "$PredictedSentiment", "count": {"$sum": 1}}},
    ]
    results = list(db["predictions"].aggregate(pipeline))
    counts  = {"positive": 0, "neutral": 0, "negative": 0}
    for r in results:
        s = (r.get("_id") or "").lower()
        if s in counts:
            counts[s] = r["count"]
    return counts


@dag(
    dag_id="daily_sentiment_digest",
    description="Daily sentiment journal with J vs J-1 comparison and health score",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["reporting", "digest", "daily"],
)
def daily_sentiment_digest():

    @task()
    def compute_today_stats() -> dict:
        client  = _mongo_client()
        db      = client[os.getenv("MONGODB_DATABASE", "amazon_reviews")]
        now     = datetime.utcnow()

        today_start      = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        today_end        = now.strftime("%Y-%m-%d %H:%M:%S")
        yesterday_start  = (now - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")

        today_counts     = _sentiment_counts(db, today_start,    today_end)
        yesterday_counts = _sentiment_counts(db, yesterday_start, today_start)

        today_total     = sum(today_counts.values())
        yesterday_total = sum(yesterday_counts.values())

        # Health score: positive=1pt, neutral=0.5pt, negative=0pt → /total * 100
        if today_total > 0:
            raw          = today_counts["positive"] + today_counts["neutral"] * 0.5
            health_score = round(raw / today_total * 100, 1)
        else:
            health_score = None

        def _pct(count, total):
            return round(count / total * 100, 1) if total > 0 else 0.0

        today_pct     = {k: _pct(today_counts[k],     today_total)     for k in today_counts}
        yesterday_pct = {k: _pct(yesterday_counts[k], yesterday_total) for k in yesterday_counts}
        delta         = {k: round(today_pct[k] - yesterday_pct[k], 1) for k in today_pct}

        client.close()
        return {
            "today_counts": today_counts, "today_total": today_total, "today_pct": today_pct,
            "yesterday_counts": yesterday_counts, "yesterday_total": yesterday_total,
            "yesterday_pct": yesterday_pct, "delta": delta,
            "health_score": health_score,
            "window_start": today_start, "window_end": today_end,
        }

    @task()
    def compute_top_products() -> list:
        client  = _mongo_client()
        db      = client[os.getenv("MONGODB_DATABASE", "amazon_reviews")]
        since   = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        pipeline = [
            {"$match": {
                "ProcessingTime": {"$gte": since, "$lt": now_str},
                "ProductId": {"$exists": True, "$ne": None},
                "PredictedSentiment": {"$exists": True},
            }},
            {"$group": {
                "_id": "$ProductId",
                "total":     {"$sum": 1},
                "positives": {"$sum": {"$cond": [{"$eq": ["$PredictedSentiment", "positive"]}, 1, 0]}},
                "negatives": {"$sum": {"$cond": [{"$eq": ["$PredictedSentiment", "negative"]}, 1, 0]}},
            }},
            {"$sort": {"total": -1}},
            {"$limit": 3},
        ]
        results = list(db["predictions"].aggregate(pipeline))
        client.close()

        return [
            {
                "product_id":    r["_id"],
                "total_reviews": r["total"],
                "positives":     r["positives"],
                "negatives":     r["negatives"],
                "satisfaction":  round(r["positives"] / r["total"] * 100, 1) if r["total"] > 0 else 0,
            }
            for r in results
        ]

    @task()
    def compute_peak_hour() -> dict:
        client  = _mongo_client()
        db      = client[os.getenv("MONGODB_DATABASE", "amazon_reviews")]
        since   = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        docs = list(db["predictions"].find(
            {"ProcessingTime": {"$gte": since, "$lt": now_str}},
            {"ProcessingTime": 1, "_id": 0},
        ))

        hour_counts = {}
        for doc in docs:
            pt = doc.get("ProcessingTime") or ""
            try:
                hour = pt[11:13]   # "YYYY-MM-DD HH:MM:SS" → slice hour
                hour_counts[hour] = hour_counts.get(hour, 0) + 1
            except Exception:
                continue

        client.close()
        if not hour_counts:
            return {"peak_hour": None, "peak_count": 0}
        peak_hour  = max(hour_counts, key=hour_counts.get)
        return {"peak_hour": f"{peak_hour}:00 UTC", "peak_count": hour_counts[peak_hour]}

    @task()
    def save_digest(stats: dict, top_products: list, peak: dict):
        client     = _mongo_client()
        db_name    = os.getenv("MONGODB_DATABASE", "amazon_reviews")
        digest_col = os.getenv("MONGO_COLLECTION_DAILY_DIGEST", "daily_digest")
        db         = client[db_name]
        today      = datetime.utcnow().strftime("%Y-%m-%d")

        doc = {
            "date":           today,
            "generated_at":   datetime.utcnow().isoformat(),
            "total_reviews":  stats["today_total"],
            "counts":         stats["today_counts"],
            "percentages":    stats["today_pct"],
            "health_score":   stats["health_score"],
            "vs_yesterday": {
                "total_reviews_yesterday": stats["yesterday_total"],
                "counts_yesterday":        stats["yesterday_counts"],
                "percentages_yesterday":   stats["yesterday_pct"],
                "delta_pct":               stats["delta"],
            },
            "top_3_products": top_products,
            "peak_hour":      peak["peak_hour"],
            "peak_count":     peak["peak_count"],
            "window_start":   stats["window_start"],
            "window_end":     stats["window_end"],
        }
        # Upsert by date to avoid duplicates on DAG reruns
        db[digest_col].replace_one({"date": today}, doc, upsert=True)
        client.close()

        delta = stats["delta"]
        print("=" * 55)
        print(f"  DAILY DIGEST — {today}")
        print("=" * 55)
        print(f"  Reviews today  : {doc['total_reviews']}")
        print(f"  Health score   : {doc['health_score']} / 100")
        print(f"  Positive       : {doc['percentages']['positive']}%  (Δ {delta['positive']:+.1f}%)")
        print(f"  Neutral        : {doc['percentages']['neutral']}%  (Δ {delta['neutral']:+.1f}%)")
        print(f"  Negative       : {doc['percentages']['negative']}%  (Δ {delta['negative']:+.1f}%)")
        print(f"  Peak hour      : {doc['peak_hour']} ({doc['peak_count']} reviews)")
        print("=" * 55)

    stats        = compute_today_stats()
    top_products = compute_top_products()
    peak         = compute_peak_hour()
    save_digest(stats, top_products, peak)


dag_instance = daily_sentiment_digest()
