"""
ai_quality_auditor.py
=====================
Runs daily at 07:00 UTC (after daily_sentiment_digest at 06:00).

Compares the original star Score with the AI PredictedSentiment
to measure live model accuracy on real streaming data.

Mapping rule (identical to spark_preprocessing_job.py):
  Score < 3  → "negative"
  Score == 3 → "neutral"
  Score > 3  → "positive"

Reports:
  - Overall live accuracy %
  - 6 named error categories
  - Per-segment accuracy (positive / neutral / negative)
  - Auto-alert if live accuracy drops below ACCURACY_ALERT_THRESHOLD

Writes one document per day to MongoDB: quality_audit
"""

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

ACCURACY_ALERT_THRESHOLD = 75.0  # alert if live accuracy < 75%


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


def _score_to_expected(score_str) -> str | None:
    """Convert raw Score string to expected sentiment label.
    Uses the EXACT same rule as spark_preprocessing_job.py.
    Returns None if Score is invalid.
    """
    try:
        score = float(score_str)
    except (TypeError, ValueError):
        return None
    if score < 3:
        return "negative"
    if score == 3:
        return "neutral"
    return "positive"


@dag(
    dag_id="ai_quality_auditor",
    description="Daily live accuracy audit: compares star Score vs AI prediction",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ml", "audit", "quality", "monitoring"],
)
def ai_quality_auditor():

    @task()
    def run_audit() -> dict:
        """
        Read last 24h from predictions, compare Score vs PredictedSentiment.
        Returns detailed accuracy metrics.
        """
        client  = _mongo_client()
        db      = client[os.getenv("MONGODB_DATABASE", "amazon_reviews")]
        since   = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        docs = list(db["predictions"].find(
            {
                "Score": {"$exists": True},
                "PredictedSentiment": {"$exists": True},
                "ProcessingTime": {"$gte": since, "$lt": now_str},
            },
            {"Score": 1, "PredictedSentiment": 1, "_id": 0},
        ))
        client.close()

        # ── Counters ──────────────────────────────────────────────────────
        total        = 0
        correct      = 0
        skipped      = 0   # invalid Score value

        # Per-segment: how many correct out of total for that segment
        segment = {
            "positive": {"correct": 0, "total": 0},
            "neutral":  {"correct": 0, "total": 0},
            "negative": {"correct": 0, "total": 0},
        }

        # 6 named error categories (expected → predicted)
        error_categories = {
            "neg_pred_as_neu":  0,   # should be negative, predicted neutral
            "neg_pred_as_pos":  0,   # should be negative, predicted positive  ← worst
            "neu_pred_as_neg":  0,   # should be neutral,  predicted negative
            "neu_pred_as_pos":  0,   # should be neutral,  predicted positive
            "pos_pred_as_neu":  0,   # should be positive, predicted neutral
            "pos_pred_as_neg":  0,   # should be positive, predicted negative  ← worst
        }

        for doc in docs:
            expected  = _score_to_expected(doc.get("Score"))
            predicted = (doc.get("PredictedSentiment") or "").lower()

            if expected is None or predicted not in ("positive", "neutral", "negative"):
                skipped += 1
                continue

            total += 1
            segment[expected]["total"] += 1

            if predicted == expected:
                correct += 1
                segment[expected]["correct"] += 1
            else:
                # Build error category key: e.g. "neg_pred_as_pos"
                short = {"negative": "neg", "neutral": "neu", "positive": "pos"}
                key = f"{short[expected]}_pred_as_{short[predicted]}"
                if key in error_categories:
                    error_categories[key] += 1

        accuracy = round(correct / total * 100, 2) if total > 0 else None

        # Per-segment accuracy %
        segment_accuracy = {
            k: round(v["correct"] / v["total"] * 100, 1) if v["total"] > 0 else None
            for k, v in segment.items()
        }

        return {
            "total_audited":    total,
            "total_skipped":    skipped,
            "correct":          correct,
            "incorrect":        total - correct,
            "live_accuracy_pct": accuracy,
            "segment_accuracy": segment_accuracy,
            "segment_counts":   {k: v["total"] for k, v in segment.items()},
            "error_categories": error_categories,
        }

    @task()
    def check_accuracy_alert(audit: dict) -> bool:
        """Fire an alert into buzz_alerts collection if accuracy drops below threshold."""
        accuracy = audit.get("live_accuracy_pct")
        if accuracy is None:
            print("⚠️  No data to evaluate today — skipping accuracy alert check")
            return False

        if accuracy < ACCURACY_ALERT_THRESHOLD:
            client     = _mongo_client()
            db_name    = os.getenv("MONGODB_DATABASE",          "amazon_reviews")
            buzz_col   = os.getenv("MONGO_COLLECTION_BUZZ_ALERTS", "buzz_alerts")
            db         = client[db_name]

            # Check cooldown: one accuracy alert per day max
            today = datetime.utcnow().strftime("%Y-%m-%d")
            recent = db[buzz_col].find_one({
                "alert_type": "LOW_ACCURACY",
                "detected_at": {"$gte": today},
            })
            if not recent:
                db[buzz_col].insert_one({
                    "alert_type":       "LOW_ACCURACY",
                    "severity":         "ALERT",
                    "detected_at":      datetime.utcnow().isoformat(),
                    "live_accuracy_pct": accuracy,
                    "threshold":        ACCURACY_ALERT_THRESHOLD,
                    "message":          (
                        f"Model live accuracy {accuracy}% is below "
                        f"threshold {ACCURACY_ALERT_THRESHOLD}%"
                    ),
                    "resolved":         False,
                })
                print(f"🚨 ACCURACY ALERT: live accuracy {accuracy}% < {ACCURACY_ALERT_THRESHOLD}%")

            client.close()
            return True

        print(f"✅ Live accuracy {accuracy}% — above threshold {ACCURACY_ALERT_THRESHOLD}%")
        return False

    @task()
    def save_audit(audit: dict, alert_fired: bool):
        """Persist the audit document to MongoDB quality_audit collection."""
        client    = _mongo_client()
        db_name   = os.getenv("MONGODB_DATABASE",           "amazon_reviews")
        audit_col = os.getenv("MONGO_COLLECTION_QUALITY_AUDIT", "quality_audit")
        db        = client[db_name]
        today     = datetime.utcnow().strftime("%Y-%m-%d")

        doc = {
            "date":             today,
            "audited_at":       datetime.utcnow().isoformat(),
            "total_audited":    audit["total_audited"],
            "total_skipped":    audit["total_skipped"],
            "correct":          audit["correct"],
            "incorrect":        audit["incorrect"],
            "live_accuracy_pct": audit["live_accuracy_pct"],
            "segment_accuracy": audit["segment_accuracy"],
            "segment_counts":   audit["segment_counts"],
            "error_categories": audit["error_categories"],
            "alert_fired":      alert_fired,
            "accuracy_threshold": ACCURACY_ALERT_THRESHOLD,
        }
        # Upsert by date
        db[audit_col].replace_one({"date": today}, doc, upsert=True)
        client.close()

        ec = audit["error_categories"]
        sa = audit["segment_accuracy"]
        print("=" * 55)
        print(f"  AI QUALITY AUDIT — {today}")
        print("=" * 55)
        print(f"  Audited        : {doc['total_audited']}  (skipped: {doc['total_skipped']})")
        print(f"  Live accuracy  : {doc['live_accuracy_pct']}%")
        print(f"  Per segment    : pos={sa['positive']}%  neu={sa['neutral']}%  neg={sa['negative']}%")
        print(f"  Worst errors   : neg→pos={ec['neg_pred_as_pos']}  pos→neg={ec['pos_pred_as_neg']}")
        print(f"  Alert fired    : {alert_fired}")
        print("=" * 55)

    audit       = run_audit()
    alert_fired = check_accuracy_alert(audit)
    save_audit(audit, alert_fired)


dag_instance = ai_quality_auditor()
