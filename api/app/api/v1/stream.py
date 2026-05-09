import asyncio
import json
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from app.core.database import get_collection, get_monthly_sentiments_collection
from app.services.analytics_service import AnalyticsService
from app.services.prediction_service import PredictionService

router = APIRouter(tags=["Streaming"])


async def stats_generator(agg_col, col):
    """Generates SSE events for global stats every 2 seconds."""
    while True:
        try:
            # For live streaming, we always use the raw collection to ensure real-time updates.
            # This reflects new predictions as soon as they are processed by Spark.
            stats = AnalyticsService.get_overall_stats(col)
            trend = AnalyticsService.get_predictions_trend(col)

            data = {
                "stats": stats.dict(),
                "trend": trend.dict()
            }
            yield f"data: {json.dumps(data)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

        await asyncio.sleep(2)


async def recent_generator(col):
    """Generates SSE events for recent predictions every 2 seconds."""
    while True:
        try:
            recent = PredictionService.get_recent_predictions(col, limit=50)
            yield f"data: {json.dumps(recent.dict())}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

        await asyncio.sleep(2)


@router.get("/stream/stats")
async def stream_stats(
    agg_col=Depends(get_monthly_sentiments_collection),
    col=Depends(get_collection),
):
    return StreamingResponse(stats_generator(agg_col, col), media_type="text/event-stream")


@router.get("/stream/recent")
async def stream_recent(col=Depends(get_collection)):
    return StreamingResponse(recent_generator(col), media_type="text/event-stream")
