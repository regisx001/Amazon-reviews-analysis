from fastapi import APIRouter, Depends
from app.core.database import (
    get_collection,
    get_monthly_sentiments_collection,
    get_product_scoring_collection,
    get_drift_status_collection,
    get_model_insights_collection,
)
from app.services.analytics_service import AnalyticsService
from app.schemas.analytics import (
    SentimentStats,
    TrendData,
    TopProduct,
    ProductSentimentList,
    DriftStatus,
    AggregationStatus,
    ModelInsights,
)

router = APIRouter(tags=["Analytics"])


@router.get("/stats", response_model=SentimentStats)
async def get_stats(
    agg_col=Depends(get_monthly_sentiments_collection),
    col=Depends(get_collection),
):
    stats = AnalyticsService.get_overall_stats_from_monthly(agg_col)
    return stats if stats.total > 0 else AnalyticsService.get_overall_stats(col)


@router.get("/predictions-by-date", response_model=TrendData)
async def get_trend(
    agg_col=Depends(get_monthly_sentiments_collection),
    col=Depends(get_collection),
):
    trend = AnalyticsService.get_predictions_trend_from_monthly(agg_col)
    return trend if trend.labels else AnalyticsService.get_predictions_trend(col)


@router.get("/top-products", response_model=TopProduct)
async def get_top_products(
    limit: int = 8,
    agg_col=Depends(get_product_scoring_collection),
    col=Depends(get_collection),
):
    top = AnalyticsService.get_top_products_from_agg(agg_col, limit)
    return top if top.labels else AnalyticsService.get_top_products(col, limit)


@router.get("/top-products-sentiment", response_model=ProductSentimentList)
async def get_top_products_sentiment(
    limit: int = 8,
    agg_col=Depends(get_product_scoring_collection),
):
    return AnalyticsService.get_product_sentiment_list(agg_col, limit)


@router.get("/drift-status", response_model=DriftStatus)
async def get_drift_status(col=Depends(get_drift_status_collection)):
    return AnalyticsService.get_latest_drift_status(col)


@router.get("/aggregation-status", response_model=AggregationStatus)
async def get_aggregation_status(
    monthly_col=Depends(get_monthly_sentiments_collection),
    product_col=Depends(get_product_scoring_collection),
):
    return AnalyticsService.get_aggregation_status(monthly_col, product_col)


@router.get("/model-insights", response_model=ModelInsights)
async def get_model_insights(
    col=Depends(get_model_insights_collection),
    pred_col=Depends(get_collection)
):
    insights = AnalyticsService.get_latest_model_insights(col)
    if insights.status == "no_data":
        return AnalyticsService.get_realtime_model_insights(pred_col)
    return insights
