from fastapi import APIRouter, Depends, Query
from app.core.database import get_collection, get_product_scoring_collection
from app.services.prediction_service import PredictionService
from app.schemas.predictions import RecentPredictionsResponse, ProductScoringResponse
from app.core.config import get_settings

router = APIRouter(tags=["Predictions"])


@router.get("/recent-predictions", response_model=RecentPredictionsResponse)
async def get_recent(limit: int = 20, col=Depends(get_collection)):
    return PredictionService.get_recent_predictions(col, limit)


@router.get("/product-scoring", response_model=ProductScoringResponse)
async def get_scoring(
    product_id: str = Query(default=None),
    col=Depends(get_collection),
    agg_col=Depends(get_product_scoring_collection),
    settings=Depends(get_settings)
):
    target_id = product_id or settings.target_product
    agg = PredictionService.get_product_scoring_from_agg(agg_col, target_id)
    return agg if agg else PredictionService.get_product_scoring(col, target_id)


@router.get("/product-scoring/{product_id}", response_model=ProductScoringResponse)
async def get_scoring_by_id(
    product_id: str,
    col=Depends(get_collection),
    agg_col=Depends(get_product_scoring_collection),
):
    agg = PredictionService.get_product_scoring_from_agg(agg_col, product_id)
    return agg if agg else PredictionService.get_product_scoring(col, product_id)
