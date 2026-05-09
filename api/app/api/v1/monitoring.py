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
    days: int = Query(default=7, le=30),
    col: Collection = Depends(get_quality_audit_collection),
):
    """
    Returns the last N days of live accuracy audits.
    Useful to track model accuracy drift over time.
    """
    docs = list(
        col.find({}, {"_id": 0})
           .sort("date", -1)
           .limit(days)
    )
    if not docs:
        return {"days": days, "status": "no_data", "audits": []}

    # Compute average accuracy over the period
    valid_acc = [d["live_accuracy_pct"] for d in docs if d.get("live_accuracy_pct") is not None]
    avg_acc   = round(sum(valid_acc) / len(valid_acc), 2) if valid_acc else None

    return {
        "days":             days,
        "average_accuracy": avg_acc,
        "latest_accuracy":  docs[0].get("live_accuracy_pct") if docs else None,
        "audits":           docs,
    }


# ── /api/recommendations ─────────────────────────────────────────────────────

@router.get("/recommendations")
async def get_recommendations(
    col: Collection = Depends(get_recommendations_collection),
):
    """
    Returns the Top 10 Recommended products (Pépites) and 
    the Top 10 Quality Alerts (Flops) based on AI sentiment analysis.
    """
    doc = col.find_one({"type": "weekly_rankings"}, {"_id": 0})
    if not doc:
        return {
            "status": "no_data",
            "message": "Recommendations have not been calculated yet. Run the weekly_product_ranking DAG.",
            "top_recommended": [],
            "quality_alerts": []
        }
    return doc
