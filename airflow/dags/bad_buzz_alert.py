"""
bad_buzz_alert.py
=================
Runs every hour — scans the last 60 minutes of predictions
and fires an alert when a product receives too many negative reviews.

Severity levels:
  WATCH    → negative ratio > 40%
  ALERT    → negative ratio > 60%
  CRITICAL → negative ratio > 80%

Anti-duplicate: a product is not re-alerted within 4 hours.
Bonus: also detects positive buzz surges (ratio > 80% positive).

Writes to MongoDB collection: buzz_alerts
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# ── Thresholds ────────────────────────────────────────────────────────────────
BAD_BUZZ_MIN_REVIEWS    = 3      # minimum reviews in window to trigger
BAD_BUZZ_WATCH_RATIO    = 0.40   # WATCH  : > 40% negative
BAD_BUZZ_ALERT_RATIO    = 0.60   # ALERT  : > 60% negative
BAD_BUZZ_CRITICAL_RATIO = 0.80   # CRITICAL: > 80% negative
GOOD_BUZZ_RATIO         = 0.80   # positive surge: > 80% positive
BAD_BUZZ_COOLDOWN_HOURS = 4      # skip re-alert within 4 hours
# ─────────────────────────────────────────────────────────────────────────────


def _mongo_client():
    """Return a connected MongoClient using env vars (same pattern as all DAGs)."""
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
    dag_id="bad_buzz_alert",
    description="Hourly product sentiment surveillance — fires alerts on negative or positive surges",
    schedule="0 * * * *",          # every hour at :00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["alerting", "product", "monitoring"],
)
def bad_buzz_alert():

    @task()
    def scan_last_hour() -> dict:
        """
        Scan the last 60 minutes of predictions.
        Group by ProductId and compute negative / positive ratios.
        Returns a list of products exceeding thresholds.
        """
        client = _mongo_client()
        db_name = os.getenv("MONGODB_DATABASE", "amazon_reviews")
        db = client[db_name]

        # ProcessingTime is stored as string "YYYY-MM-DD HH:MM:SS" (UTC)
        one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "ProcessingTime": {"$gte": one_hour_ago},
                    "PredictedSentiment": {"$exists": True},
                    "ProductId": {"$exists": True, "$ne": None},
                }
            },
            {
                "$group": {
                    "_id": "$ProductId",
                    "total": {"$sum": 1},
                    "negatives": {
                        "$sum": {
                            "$cond": [{"$eq": ["$PredictedSentiment", "negative"]}, 1, 0]
                        }
                    },
                    "positives": {
                        "$sum": {
                            "$cond": [{"$eq": ["$PredictedSentiment", "positive"]}, 1, 0]
                        }
                    },
                }
            },
            {"$match": {"total": {"$gte": BAD_BUZZ_MIN_REVIEWS}}},
            {
                "$addFields": {
                    "neg_ratio": {"$divide": ["$negatives", "$total"]},
                    "pos_ratio": {"$divide": ["$positives", "$total"]},
                }
            },
            # Keep only products with notable buzz (bad or good)
            {
                "$match": {
                    "$or": [
                        {"neg_ratio": {"$gte": BAD_BUZZ_WATCH_RATIO}},
                        {"pos_ratio": {"$gte": GOOD_BUZZ_RATIO}},
                    ]
                }
            },
        ]

        products = list(db["predictions"].aggregate(pipeline))
        client.close()

        print(f"[SCAN] {len(products)} product(s) with notable buzz in the last hour")
        # Serialize for XCom (ObjectId → string not needed here, plain dicts)
        return {
            "products": [
                {
                    "product_id": p["_id"],
                    "total":      p["total"],
                    "negatives":  p["negatives"],
                    "positives":  p["positives"],
                    "neg_ratio":  p["neg_ratio"],
                    "pos_ratio":  p["pos_ratio"],
                }
                for p in products
            ],
            "window_start": one_hour_ago,
        }

    @task()
    def fire_alerts(scan_result: dict) -> dict:
        """
        For each flagged product:
        - Determine severity (WATCH / ALERT / CRITICAL) or POSITIVE_SURGE
        - Skip if a recent alert exists (cooldown)
        - Insert alert document into buzz_alerts collection
        """
        client = _mongo_client()
        db_name = os.getenv("MONGODB_DATABASE", "amazon_reviews")
        buzz_col = os.getenv("MONGO_COLLECTION_BUZZ_ALERTS", "buzz_alerts")
        db = client[db_name]

        products      = scan_result.get("products", [])
        window_start  = scan_result.get("window_start", "")
        cooldown_time = (datetime.utcnow() - timedelta(hours=BAD_BUZZ_COOLDOWN_HOURS)).isoformat()

        fired    = 0
        skipped  = 0
        resolved = 0

        for p in products:
            product_id = p["product_id"]
            neg_ratio  = p["neg_ratio"]
            pos_ratio  = p["pos_ratio"]

            # ── Determine alert type and severity ──────────────────────────
            if neg_ratio >= BAD_BUZZ_CRITICAL_RATIO:
                alert_type = "BAD_BUZZ"
                severity   = "CRITICAL"
            elif neg_ratio >= BAD_BUZZ_ALERT_RATIO:
                alert_type = "BAD_BUZZ"
                severity   = "ALERT"
            elif neg_ratio >= BAD_BUZZ_WATCH_RATIO:
                alert_type = "BAD_BUZZ"
                severity   = "WATCH"
            elif pos_ratio >= GOOD_BUZZ_RATIO:
                alert_type = "POSITIVE_SURGE"
                severity   = "INFO"
            else:
                continue

            # ── Anti-duplicate cooldown ────────────────────────────────────
            recent = db[buzz_col].find_one({
                "product_id": product_id,
                "alert_type": alert_type,
                "detected_at": {"$gte": cooldown_time},
            })
            if recent:
                print(f"[SKIP] {product_id} — {alert_type} already fired within {BAD_BUZZ_COOLDOWN_HOURS}h")
                skipped += 1
                continue

            # ── Insert alert ───────────────────────────────────────────────
            doc = {
                "product_id":    product_id,
                "alert_type":    alert_type,
                "severity":      severity,
                "detected_at":   datetime.utcnow().isoformat(),
                "window_start":  window_start,
                "window_hours":  1,
                "total_reviews": p["total"],
                "negatives":     p["negatives"],
                "positives":     p["positives"],
                "neg_ratio_pct": round(neg_ratio * 100, 1),
                "pos_ratio_pct": round(pos_ratio * 100, 1),
                "resolved":      False,
            }
            db[buzz_col].insert_one(doc)
            fired += 1

            emoji = "🚨" if alert_type == "BAD_BUZZ" else "🌟"
            print(
                f"{emoji} [{severity}] {product_id} — "
                f"{doc['neg_ratio_pct'] if alert_type == 'BAD_BUZZ' else doc['pos_ratio_pct']}% "
                f"{'negative' if alert_type == 'BAD_BUZZ' else 'positive'} "
                f"on {p['total']} reviews"
            )

        client.close()
        return {"fired": fired, "skipped": skipped, "resolved": resolved}

    @task()
    def resolve_old_alerts() -> int:
        """
        Mark as resolved any BAD_BUZZ alerts for products
        that are no longer in bad standing in the last hour.
        This closes the alert lifecycle cleanly.
        """
        client = _mongo_client()
        db_name = os.getenv("MONGODB_DATABASE", "amazon_reviews")
        buzz_col = os.getenv("MONGO_COLLECTION_BUZZ_ALERTS", "buzz_alerts")
        db = client[db_name]

        one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Products still bad in last hour
        pipeline = [
            {"$match": {
                "ProcessingTime": {"$gte": one_hour_ago},
                "PredictedSentiment": "negative",
                "ProductId": {"$exists": True}
            }},
            {"$group": {"_id": "$ProductId", "cnt": {"$sum": 1}}},
        ]
        still_bad = {r["_id"] for r in db["predictions"].aggregate(pipeline)}

        # Resolve open alerts for products no longer flagged
        result = db[buzz_col].update_many(
            {
                "alert_type": "BAD_BUZZ",
                "resolved":   False,
                "product_id": {"$nin": list(still_bad)},
            },
            {"$set": {"resolved": True, "resolved_at": datetime.utcnow().isoformat()}},
        )
        client.close()
        if result.modified_count > 0:
            print(f"✅ Resolved {result.modified_count} alert(s) — products returned to normal")
        return result.modified_count

    @task()
    def summary_report(alert_result: dict, resolved_count: int):
        print("=" * 50)
        print("  HOURLY BAD BUZZ REPORT")
        print("=" * 50)
        print(f"  Alerts fired   : {alert_result['fired']}")
        print(f"  Alerts skipped : {alert_result['skipped']} (cooldown)")
        print(f"  Alerts resolved: {resolved_count}")
        print(f"  Timestamp      : {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        print("=" * 50)

    scan    = scan_last_hour()
    alerts  = fire_alerts(scan)
    resolve = resolve_old_alerts()
    summary_report(alerts, resolve)


dag_instance = bad_buzz_alert()
