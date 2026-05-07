from pydantic import BaseModel
from typing import Dict, List, Optional, Any


class SentimentStats(BaseModel):
    total: int
    positive: int
    neutral: int
    negative: int


class TrendData(BaseModel):
    labels: List[str]
    positive: List[int]
    neutral: List[int]
    negative: List[int]


class TopProduct(BaseModel):
    labels: List[str]
    counts: List[int]


class ProductSentimentCounts(BaseModel):
    positive: int
    neutral: int
    negative: int


class ProductSentimentPercentages(BaseModel):
    positive: float
    neutral: float
    negative: float


class ProductSentimentItem(BaseModel):
    product_id: str
    total: int
    counts: ProductSentimentCounts
    percentages: ProductSentimentPercentages


class ProductSentimentList(BaseModel):
    products: List[ProductSentimentItem]


class DriftCounts(BaseModel):
    positive: int
    neutral: int
    negative: int


class DriftStatus(BaseModel):
    evaluated_at: Optional[str]
    total: int
    neutral_ratio: float
    negative_ratio: float
    drift_detected: bool
    alerts: List[str]
    counts: DriftCounts
    reason: Optional[str] = None


class AggregationStatus(BaseModel):
    last_aggregated_at: Optional[str]
    monthly_last_aggregated_at: Optional[str]
    product_last_aggregated_at: Optional[str]


class ModelClassMetrics(BaseModel):
    precision: float
    recall: float
    f1: float


class ModelMetrics(BaseModel):
    accuracy: Optional[float] = None
    f1_weighted: Optional[float] = None
    precision_weighted: Optional[float] = None
    recall_weighted: Optional[float] = None
    per_class: Optional[Dict[str, ModelClassMetrics]] = None


class ConfusionMatrix(BaseModel):
    labels: List[str]
    matrix: List[List[int]]


class ModelMetadata(BaseModel):
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    trained_at: Optional[str] = None
    dataset: Optional[str] = None
    notes: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None


class ModelInsights(BaseModel):
    evaluated_at: Optional[str]
    status: str
    metrics: Optional[ModelMetrics] = None
    confusion_matrix: Optional[ConfusionMatrix] = None
    metadata: Optional[ModelMetadata] = None
    source: Optional[Dict[str, str]] = None
    errors: List[str] = []
