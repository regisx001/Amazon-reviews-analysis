from collections import defaultdict
from datetime import datetime
from pymongo.collection import Collection
from ..schemas.analytics import (
    SentimentStats,
    TrendData,
    TopProduct,
    ProductSentimentList,
    ProductSentimentItem,
    ProductSentimentCounts,
    ProductSentimentPercentages,
    DriftStatus,
    DriftCounts,
    AggregationStatus,
    ModelInsights,
    ModelMetrics,
    ModelClassMetrics,
    ConfusionMatrix,
    ModelMetadata,
)


class AnalyticsService:
    @staticmethod
    def get_overall_stats(collection: Collection) -> SentimentStats:
        pipeline = [
            {"$group": {"_id": "$PredictedSentiment", "count": {"$sum": 1}}}
        ]
        results = list(collection.aggregate(pipeline))
        counts = {"positive": 0, "neutral": 0, "negative": 0}
        for r in results:
            s = (r.get("_id") or "").lower()
            if s in counts:
                counts[s] = r["count"]
        total = sum(counts.values())
        return SentimentStats(total=total, **counts)

    @staticmethod
    def get_predictions_trend(collection: Collection) -> TrendData:
        docs = collection.find(
            {"PredictedSentiment": {"$exists": True}},
            {"ProcessingTime": 1, "Time": 1, "PredictedSentiment": 1, "_id": 0},
        )

        monthly = defaultdict(
            lambda: {"positive": 0, "neutral": 0, "negative": 0})

        for doc in docs:
            ts = doc.get("ProcessingTime") or doc.get("Time")
            sentiment = (doc.get("PredictedSentiment") or "").lower()
            if not ts or sentiment not in ("positive", "neutral", "negative"):
                continue
            try:
                if isinstance(ts, (int, float)):
                    dt = datetime.utcfromtimestamp(float(ts))
                else:
                    ts_str = str(ts).strip()
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
                        try:
                            dt = datetime.strptime(ts_str[:19], fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        continue
                month_key = dt.strftime("%b'%y")
                monthly[month_key][sentiment] += 1
            except:
                continue

        sorted_months = sorted(monthly.keys(), key=lambda k: datetime.strptime(
            k, "%b'%y") if "%b'%y" in k else datetime.min)
        return TrendData(
            labels=sorted_months,
            positive=[monthly[m]["positive"] for m in sorted_months],
            neutral=[monthly[m]["neutral"] for m in sorted_months],
            negative=[monthly[m]["negative"] for m in sorted_months]
        )

    @staticmethod
    def get_top_products(collection: Collection, limit: int = 8) -> TopProduct:
        pipeline = [
            {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
        results = list(collection.aggregate(pipeline))
        return TopProduct(
            labels=[r.get("_id") or "Unknown" for r in results],
            counts=[r["count"] for r in results]
        )

    @staticmethod
    def get_overall_stats_from_monthly(collection: Collection) -> SentimentStats:
        pipeline = [
            {"$match": {"aggregated_at": {"$exists": True}}},
            {"$group": {"_id": "$_id.sentiment", "count": {"$sum": "$count"}}}
        ]
        results = list(collection.aggregate(pipeline))
        counts = {"positive": 0, "neutral": 0, "negative": 0}
        for r in results:
            s = (r.get("_id") or "").lower()
            if s in counts:
                counts[s] = int(r.get("count") or 0)
        total = sum(counts.values())
        return SentimentStats(total=total, **counts)

    @staticmethod
    def get_predictions_trend_from_monthly(collection: Collection) -> TrendData:
        docs = collection.find(
            {"aggregated_at": {"$exists": True}},
            {"_id": 1, "count": 1},
        )
        monthly = defaultdict(
            lambda: {"positive": 0, "neutral": 0, "negative": 0})
        month_dates = {}

        for doc in docs:
            key = doc.get("_id") or {}
            year = key.get("year")
            month = key.get("month")
            sentiment = (key.get("sentiment") or "").lower()
            if not year or not month or sentiment not in ("positive", "neutral", "negative"):
                continue
            try:
                dt = datetime(int(year), int(month), 1)
            except ValueError:
                continue
            month_key = dt.strftime("%b'%y")
            monthly[month_key][sentiment] += int(doc.get("count") or 0)
            month_dates[month_key] = dt

        sorted_months = sorted(
            month_dates.keys(), key=lambda k: month_dates[k])
        return TrendData(
            labels=sorted_months,
            positive=[monthly[m]["positive"] for m in sorted_months],
            neutral=[monthly[m]["neutral"] for m in sorted_months],
            negative=[monthly[m]["negative"] for m in sorted_months],
        )

    @staticmethod
    def get_top_products_from_agg(collection: Collection, limit: int = 8) -> TopProduct:
        docs = list(
            collection.find(
                {"aggregated_at": {"$exists": True}},
                {"_id": 1, "total": 1},
            ).sort("total", -1).limit(limit)
        )
        return TopProduct(
            labels=[d.get("_id") or "Unknown" for d in docs],
            counts=[int(d.get("total") or 0) for d in docs],
        )

    @staticmethod
    def get_product_sentiment_list(collection: Collection, limit: int = 8) -> ProductSentimentList:
        docs = list(
            collection.find(
                {"aggregated_at": {"$exists": True}},
                {"_id": 1, "sentiments": 1, "total": 1},
            )
            .sort("total", -1)
            .limit(limit)
        )
        items = []
        for doc in docs:
            counts = {"positive": 0, "neutral": 0, "negative": 0}
            for entry in doc.get("sentiments") or []:
                s = (entry.get("sentiment") or "").lower()
                if s in counts:
                    counts[s] = int(entry.get("count") or 0)
            total = int(doc.get("total") or sum(counts.values()))
            percentages = {
                k: round(v / total * 100, 1) if total > 0 else 0 for k, v in counts.items()
            }
            items.append(
                ProductSentimentItem(
                    product_id=doc.get("_id") or "Unknown",
                    total=total,
                    counts=ProductSentimentCounts(**counts),
                    percentages=ProductSentimentPercentages(**percentages),
                )
            )
        return ProductSentimentList(products=items)

    @staticmethod
    def get_latest_drift_status(collection: Collection) -> DriftStatus:
        doc = (
            collection.find({}, {"_id": 0})
            .sort("evaluated_at", -1)
            .limit(1)
        )
        latest = next(doc, None)
        if not latest:
            return DriftStatus(
                evaluated_at=None,
                total=0,
                neutral_ratio=0.0,
                negative_ratio=0.0,
                drift_detected=False,
                alerts=[],
                counts=DriftCounts(positive=0, neutral=0, negative=0),
                reason="no_data",
            )

        counts = latest.get("counts") or {}
        counts_dict = {
            "positive": int(counts.get("positive", 0)),
            "neutral": int(counts.get("neutral", 0)),
            "negative": int(counts.get("negative", 0)),
        }
        total = int(latest.get("total") or sum(counts_dict.values()))
        neutral_ratio = latest.get("neutral_ratio")
        negative_ratio = latest.get("negative_ratio")
        if neutral_ratio is None:
            neutral_ratio = counts_dict["neutral"] / \
                total if total > 0 else 0.0
        if negative_ratio is None:
            negative_ratio = counts_dict["negative"] / \
                total if total > 0 else 0.0

        evaluated_at = latest.get("evaluated_at")
        if isinstance(evaluated_at, datetime):
            evaluated_at_str = evaluated_at.isoformat()
        elif evaluated_at:
            evaluated_at_str = str(evaluated_at)
        else:
            evaluated_at_str = None

        return DriftStatus(
            evaluated_at=evaluated_at_str,
            total=total,
            neutral_ratio=float(neutral_ratio),
            negative_ratio=float(negative_ratio),
            drift_detected=bool(latest.get("drift_detected", False)),
            alerts=list(latest.get("alerts") or []),
            counts=DriftCounts(**counts_dict),
            reason=latest.get("reason"),
        )

    @staticmethod
    def get_aggregation_status(
        monthly_collection: Collection,
        product_collection: Collection,
    ) -> AggregationStatus:
        def latest_dt(col: Collection) -> datetime | None:
            doc = (
                col.find({}, {"aggregated_at": 1, "_id": 0})
                .sort("aggregated_at", -1)
                .limit(1)
            )
            latest = next(doc, None)
            value = latest.get("aggregated_at") if latest else None
            return value if isinstance(value, datetime) else None

        monthly_dt = latest_dt(monthly_collection)
        product_dt = latest_dt(product_collection)

        last_dt = None
        if monthly_dt and product_dt:
            last_dt = max(monthly_dt, product_dt)
        else:
            last_dt = monthly_dt or product_dt

        return AggregationStatus(
            last_aggregated_at=last_dt.isoformat() if last_dt else None,
            monthly_last_aggregated_at=monthly_dt.isoformat() if monthly_dt else None,
            product_last_aggregated_at=product_dt.isoformat() if product_dt else None,
        )

    @staticmethod
    def get_latest_model_insights(collection: Collection) -> ModelInsights:
        doc = (
            collection.find({}, {"_id": 0})
            .sort("evaluated_at", -1)
            .limit(1)
        )
        latest = next(doc, None)
        if not latest:
            return ModelInsights(evaluated_at=None, status="no_data", errors=[])

        evaluated_at = latest.get("evaluated_at")
        if isinstance(evaluated_at, datetime):
            evaluated_at_str = evaluated_at.isoformat()
        elif evaluated_at:
            evaluated_at_str = str(evaluated_at)
        else:
            evaluated_at_str = None

        metrics_data = latest.get("metrics") or None
        metrics = None
        if isinstance(metrics_data, dict):
            per_class = metrics_data.get("per_class")
            if isinstance(per_class, dict):
                per_class = {
                    k: ModelClassMetrics(**v) for k, v in per_class.items() if isinstance(v, dict)
                }
            metrics = ModelMetrics(
                accuracy=metrics_data.get("accuracy"),
                f1_weighted=metrics_data.get("f1_weighted"),
                precision_weighted=metrics_data.get("precision_weighted"),
                recall_weighted=metrics_data.get("recall_weighted"),
                per_class=per_class,
            )

        matrix_data = latest.get("confusion_matrix") or None
        confusion_matrix = None
        if isinstance(matrix_data, dict) and "labels" in matrix_data and "matrix" in matrix_data:
            confusion_matrix = ConfusionMatrix(
                labels=list(matrix_data.get("labels") or []),
                matrix=list(matrix_data.get("matrix") or []),
            )

        metadata_data = latest.get("metadata") or None
        metadata = None
        if isinstance(metadata_data, dict):
            metadata = ModelMetadata(
                model_name=metadata_data.get("model_name"),
                model_version=metadata_data.get("model_version"),
                trained_at=metadata_data.get("trained_at"),
                dataset=metadata_data.get("dataset"),
                notes=metadata_data.get("notes"),
                parameters=metadata_data.get("parameters"),
            )

        return ModelInsights(
            evaluated_at=evaluated_at_str,
            status=latest.get("status", "ok"),
            metrics=metrics,
            confusion_matrix=confusion_matrix,
            metadata=metadata,
            source=latest.get("source"),
            errors=list(latest.get("errors") or []),
        )
