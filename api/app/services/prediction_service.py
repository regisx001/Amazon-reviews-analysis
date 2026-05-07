from pymongo.collection import Collection
from ..schemas.predictions import (
    RecentPredictionsResponse,
    ProductScoringResponse,
    ProductScoringCounts,
    ProductScoringPercentages
)


class PredictionService:
    @staticmethod
    def get_recent_predictions(collection: Collection, limit: int = 20) -> RecentPredictionsResponse:
        docs = list(
            collection.find(
                {"PredictedSentiment": {"$exists": True}},
                {"_id": 0, "Id": 1, "ProductId": 1, "ProfileName": 1, "Score": 1,
                    "PredictedSentiment": 1, "Summary": 1, "ProcessingTime": 1}
            ).sort("_id", -1).limit(limit)
        )
        return RecentPredictionsResponse(predictions=docs)

    @staticmethod
    def get_product_scoring(collection: Collection, product_id: str) -> ProductScoringResponse:
        pipeline = [
            {"$match": {"ProductId": product_id,
                        "PredictedSentiment": {"$exists": True}}},
            {"$group": {"_id": "$PredictedSentiment", "count": {"$sum": 1}}},
        ]
        results = list(collection.aggregate(pipeline))
        counts_dict = {"positive": 0, "neutral": 0, "negative": 0}
        for r in results:
            s = (r.get("_id") or "").lower()
            if s in counts_dict:
                counts_dict[s] = r["count"]

        total = sum(counts_dict.values())
        percentages = {
            k: round(v / total * 100, 1) if total > 0 else 0
            for k, v in counts_dict.items()
        }

        return ProductScoringResponse(
            product_id=product_id,
            total=total,
            counts=ProductScoringCounts(**counts_dict),
            percentages=ProductScoringPercentages(**percentages)
        )

    @staticmethod
    def get_product_scoring_from_agg(collection: Collection, product_id: str) -> ProductScoringResponse | None:
        doc = collection.find_one(
            {"_id": product_id},
            {"_id": 1, "sentiments": 1, "total": 1},
        )
        if not doc:
            return None

        counts_dict = {"positive": 0, "neutral": 0, "negative": 0}
        for entry in doc.get("sentiments") or []:
            s = (entry.get("sentiment") or "").lower()
            if s in counts_dict:
                counts_dict[s] = int(entry.get("count") or 0)

        total = int(doc.get("total") or sum(counts_dict.values()))
        percentages = {
            k: round(v / total * 100, 1) if total > 0 else 0
            for k, v in counts_dict.items()
        }

        return ProductScoringResponse(
            product_id=product_id,
            total=total,
            counts=ProductScoringCounts(**counts_dict),
            percentages=ProductScoringPercentages(**percentages)
        )
