import os
from dataclasses import dataclass
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    base_dir: str
    mongo_uri: str
    mongo_db: str
    mongo_collection: str
    mongo_monthly_collection: str
    mongo_product_scoring_collection: str
    mongo_drift_collection: str
    mongo_model_insights_collection: str
    mongo_buzz_alerts_collection: str
    mongo_daily_digest_collection: str
    mongo_quality_audit_collection: str
    mongo_recommendations_collection: str
    target_product: str
    frontend_origins: list[str]
    static_dir: str
    templates_dir: str


@lru_cache
def get_settings() -> Settings:
    core_dir = os.path.dirname(os.path.abspath(__file__))
    app_dir = os.path.dirname(core_dir)
    base_dir = os.path.dirname(app_dir)

    mongo_uri = os.getenv("MONGO_URI") or (
        "mongodb://{user}:{pw}@{host}:{port}/".format(
            user=os.getenv("MONGODB_ROOT_USER", "admin"),
            pw=os.getenv("MONGODB_ROOT_PASSWORD", "admin123"),
            host=os.getenv("MONGODB_HOST", "mongodb"),
            port=os.getenv("MONGODB_PORT", "27017"),
        )
    )

    frontend_origins_env = os.getenv(
        "FRONTEND_ORIGINS", "http://localhost:5173")
    frontend_origins = [o.strip()
                        for o in frontend_origins_env.split(",") if o.strip()]

    return Settings(
        base_dir=base_dir,
        mongo_uri=mongo_uri,
        mongo_db=os.getenv("MONGODB_DATABASE", "amazon_reviews"),
        mongo_collection=os.getenv("MONGO_COLLECTION", "predictions"),
        mongo_monthly_collection=os.getenv(
            "MONGO_COLLECTION_MONTHLY", "agg_monthly_sentiments"
        ),
        mongo_product_scoring_collection=os.getenv(
            "MONGO_COLLECTION_PRODUCT_SCORING", "agg_product_scoring"
        ),
        mongo_drift_collection=os.getenv(
            "MONGO_COLLECTION_DRIFT", "model_drift_status"
        ),
        mongo_model_insights_collection=os.getenv(
            "MONGO_COLLECTION_MODEL_INSIGHTS", "model_insights"
        ),
        mongo_buzz_alerts_collection=os.getenv(
            "MONGO_COLLECTION_BUZZ_ALERTS", "buzz_alerts"
        ),
        mongo_daily_digest_collection=os.getenv(
            "MONGO_COLLECTION_DAILY_DIGEST", "daily_digest"
        ),
        mongo_quality_audit_collection=os.getenv(
            "MONGO_COLLECTION_QUALITY_AUDIT", "quality_audit"
        ),
        mongo_recommendations_collection=os.getenv(
            "MONGO_COLLECTION_RECOMMENDATIONS", "product_rankings"
        ),
        target_product=os.getenv("TARGET_PRODUCT", "B001E4KFG0"),
        frontend_origins=frontend_origins,
        static_dir=os.path.join(base_dir, "static"),
        templates_dir=app_dir,
    )
