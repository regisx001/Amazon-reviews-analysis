from pymongo import MongoClient

from .config import get_settings

_client: MongoClient | None = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        settings = get_settings()
        _client = MongoClient(settings.mongo_uri,
                              serverSelectionTimeoutMS=5000)
    return _client


def get_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_collection]


def get_monthly_sentiments_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_monthly_collection]


def get_product_scoring_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_product_scoring_collection]


def get_drift_status_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_drift_collection]


def get_model_insights_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_model_insights_collection]


def get_buzz_alerts_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_buzz_alerts_collection]


def get_daily_digest_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_daily_digest_collection]


def get_quality_audit_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_quality_audit_collection]


def get_recommendations_collection():
    settings = get_settings()
    return get_client()[settings.mongo_db][settings.mongo_recommendations_collection]
