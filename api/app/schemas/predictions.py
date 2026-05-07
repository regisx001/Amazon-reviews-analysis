from pydantic import BaseModel, Field
from typing import List, Optional, Union

class PredictionItem(BaseModel):
    Id: Optional[str] = None
    ProductId: Optional[str] = None
    ProfileName: Optional[str] = None
    Score: Optional[Union[int, float, str]] = None
    PredictedSentiment: Optional[str] = None
    Summary: Optional[str] = None
    ProcessingTime: Optional[Union[str, int, float]] = None

class RecentPredictionsResponse(BaseModel):
    predictions: List[PredictionItem]

class ProductScoringCounts(BaseModel):
    positive: int
    neutral: int
    negative: int

class ProductScoringPercentages(BaseModel):
    positive: float
    neutral: float
    negative: float

class ProductScoringResponse(BaseModel):
    product_id: str
    total: int
    counts: ProductScoringCounts
    percentages: ProductScoringPercentages
