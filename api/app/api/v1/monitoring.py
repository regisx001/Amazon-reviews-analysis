"""
monitoring.py — API v1 router
Exposes the 3 new DAG outputs as REST endpoints:
  GET /api/buzz-alerts          → active & recent product alerts
  GET /api/daily-digest         → today's sentiment journal
  GET /api/daily-digest/history → last N days
  GET /api/model-live-accuracy  → quality audit history
"""

from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, Query
from pymongo.collection import Collection

from app.core.database import (
    get_buzz_alerts_collection,
    get_daily_digest_collection,
    get_quality_audit_collection,
    get_recommendations_collection,
    get_predictions_collection,
)

router = APIRouter(tags=["Monitoring"])


# ── /api/buzz-alerts ─────────────────────────────────────────────────────────

@router.get("/buzz-alerts")
async def get_buzz_alerts(
    active_only: bool = Query(default=False, description="Return only unresolved alerts"),
    limit: int = Query(default=20, le=100),
    col: Collection = Depends(get_buzz_alerts_collection),
):
    """
    Returns recent product buzz alerts (bad buzz or positive surges).
    Set active_only=true to filter only unresolved alerts.
    """
    query = {}
    if active_only:
        query["resolved"] = False

    docs = list(
        col.find(query, {"_id": 0})
           .sort("detected_at", -1)
           .limit(limit)
    )
    # Summary counts
    total_active   = col.count_documents({"resolved": False, "alert_type": "BAD_BUZZ"})
    total_positive = col.count_documents({"resolved": False, "alert_type": "POSITIVE_SURGE"})

    return {
        "active_bad_buzz":     total_active,
        "active_positive_surge": total_positive,
        "alerts": docs,
    }


@router.get("/buzz-alerts/product/{product_id}")
async def get_product_alerts(
    product_id: str,
    col: Collection = Depends(get_buzz_alerts_collection),
):
    """Returns all alerts for a specific product (most recent first)."""
    docs = list(
        col.find({"product_id": product_id}, {"_id": 0})
           .sort("detected_at", -1)
           .limit(50)
    )
    return {"product_id": product_id, "alerts": docs, "total": len(docs)}


# ── /api/daily-digest ────────────────────────────────────────────────────────

@router.get("/daily-digest")
async def get_daily_digest(
    date: Optional[str] = Query(default=None, description="Date YYYY-MM-DD, defaults to today"),
    col: Collection = Depends(get_daily_digest_collection),
):
    """Returns the sentiment digest for a given day (defaults to today)."""
    target_date = date or datetime.utcnow().strftime("%Y-%m-%d")
    doc = col.find_one({"date": target_date}, {"_id": 0})
    if not doc:
        return {"date": target_date, "status": "not_available", "message": "Digest not yet generated for this date"}
    return doc


@router.get("/daily-digest/history")
async def get_digest_history(
    days: int = Query(default=7, le=30),
    col: Collection = Depends(get_daily_digest_collection),
):
    """Returns the last N days of digests (most recent first)."""
    docs = list(
        col.find({}, {"_id": 0, "date": 1, "total_reviews": 1, "health_score": 1,
                       "percentages": 1, "vs_yesterday.delta_pct": 1})
           .sort("date", -1)
           .limit(days)
    )
    return {"days": days, "history": docs}


# ── /api/model-live-accuracy ─────────────────────────────────────────────────

@router.get("/model-live-accuracy")
async def get_model_live_accuracy(
    limit: int = Query(default=1000, le=5000),
    col: Collection = Depends(get_predictions_collection),
):
    """
    Calculates model accuracy in REAL-TIME by comparing human stars (Score)
    with AI predictions.
    """
    pipeline = [
        # 1. Project the comparison logic
        {
            "$project": {
                "PredictedSentiment": 1,
                "HumanSentiment": {
                    "$cond": [
                        {"$lte": ["$Score", 2]}, "negative",
                        {"$cond": [
                            {"$eq": ["$Score", 3]}, "neutral", "positive"
                        ]}
                    ]
                }
            }
        },
        # 2. Group to count matches
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "correct": {
                    "$sum": {
                        "$cond": [{"$eq": ["$PredictedSentiment", "$HumanSentiment"]}, 1, 0]
                    }
                }
            }
        }
    ]

    results = list(col.aggregate(pipeline))
    
    if not results or results[0]["total"] == 0:
        return {
            "status": "no_data",
            "accuracy_pct": 0,
            "total_samples": 0,
            "message": "Start the stream to see real-time accuracy."
        }

    stats = results[0]
    accuracy = round((stats["correct"] / stats["total"]) * 100, 2)

    return {
        "status": "success",
        "accuracy_pct": accuracy,
        "total_samples": stats["total"],
        "generated_at": datetime.utcnow().isoformat()
    }


# ── /api/recommendations ─────────────────────────────────────────────────────

@router.get("/recommendations")
async def get_recommendations(
    col_predictions: Collection = Depends(get_predictions_collection),
):
    """
    Calculates the Top 10 Recommended and Top 10 Flops in REAL-TIME
    based on all predictions stored in MongoDB.
    """
    pipeline = [
        # 1. Group by ProductId
        {
            "$group": {
                "_id": "$ProductId",
                "total_reviews": {"$sum": 1},
                "avg_human_score": {"$avg": "$Score"},
                "positives": {
                    "$sum": {"$cond": [{"$eq": ["$PredictedSentiment", "positive"]}, 1, 0]}
                },
                "negatives": {
                    "$sum": {"$cond": [{"$eq": ["$PredictedSentiment", "negative"]}, 1, 0]}
                },
                # Get the last product name/summary seen
                "sample_summary": {"$first": "$Summary"}
            }
        },
        # 2. Filter: Only products with at least 3 reviews to ensure confidence
        {"$match": {"total_reviews": {"$gte": 3}}},
        # 3. Calculate Satisfaction Score
        {
            "$addFields": {
                "satisfaction_score": {"$multiply": [{"$divide": ["$positives", "$total_reviews"]}, 100]},
                "disappointment_score": {"$multiply": [{"$divide": ["$negatives", "$total_reviews"]}, 100]}
            }
        }
    ]

    results = list(col_predictions.aggregate(pipeline))

    # Sort: Primary key is the score, secondary key is the volume (total_reviews)
    # This ensures that a 100% with 10 reviews is better than a 100% with 3 reviews.
    top_recommended = sorted(
        results, 
        key=lambda x: (x['satisfaction_score'], x['total_reviews']), 
        reverse=True
    )[:10]

    quality_alerts = sorted(
        results, 
        key=lambda x: (x['disappointment_score'], x['total_reviews']), 
        reverse=True
    )[:10]

    return {
        "status": "success",
        "type": "real_time_rankings",
        "generated_at": datetime.utcnow().isoformat(),
        "top_recommended": [
            {
                "product_id": item["_id"],
                "summary": item["sample_summary"],
                "satisfaction_pct": round(item["satisfaction_score"], 1),
                "avg_human_score": round(item.get("avg_human_score", 0), 1),
                "total_reviews": item["total_reviews"]
            } for item in top_recommended
        ],
        "quality_alerts": [
            {
                "product_id": item["_id"],
                "summary": item["sample_summary"],
                "disappointment_pct": round(item["disappointment_score"], 1),
                "avg_human_score": round(item.get("avg_human_score", 0), 1),
                "total_reviews": item["total_reviews"]
            } for item in quality_alerts
        ]
    }
