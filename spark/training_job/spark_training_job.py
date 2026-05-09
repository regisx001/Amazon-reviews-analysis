"""
Spark Training Job — Logistic Regression (Best Model)
======================================================
1. Load train_feat (parquet)
2. Train LogisticRegression model
3. Save best_lr model and metadata
"""

import json
import os
import time
from datetime import datetime, timezone

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

# ── CONFIG ────────────────────────────────────────────────────────────────────
DATA_DIR    = "/opt/spark/work-dir/data"
OUTPUT_DIR  = os.path.join(DATA_DIR, "output")
TRAIN_FEAT  = os.path.join(OUTPUT_DIR, "train_feat")
MODEL_DIR   = os.path.join(OUTPUT_DIR, "models", "best_lr")
META_PATH   = os.path.join(OUTPUT_DIR, "models", "training_meta.json")

NUM_FEATURES = 100_000
# ─────────────────────────────────────────────────────────────────────────────

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("AmazonReviews-BestLR-Training")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"✓ Spark {spark.version} ready.")


# ── 1. Load Data ──────────────────────────────────────────────────────────────
print("Loading train features...")
train_feat = spark.read.parquet(TRAIN_FEAT)
print(f"Train features: {train_feat.count():,} rows")

# ── 2. Train LogisticRegression ───────────────────────────────────────────────
# Best hyperparameters identified from offline tuning (notebook 04):
#   regParam=0.001, elasticNetParam=0.0, maxIter=200
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    weightCol="classWeight",
    family="multinomial",
    regParam=0.001,
    elasticNetParam=0.0,
    maxIter=200,
)

print("Training Logistic Regression…")
t0 = time.time()
lr_model = lr.fit(train_feat)
print(f"✓ Training done in {time.time() - t0:.1f}s")


# ── 3. Save model artefacts ───────────────────────────────────────────────────
os.makedirs(os.path.join(OUTPUT_DIR, "models"), exist_ok=True)
try:
    os.chmod(os.path.join(OUTPUT_DIR, "models"), 0o777)
except:
    pass

lr_model.write().overwrite().save(MODEL_DIR)
print(f"\n✓ LR model saved → {MODEL_DIR}")

# Persist hyperparameters / training metadata for downstream jobs
meta = {
    "trained_at":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "model_name":    "LogisticRegression",
    "model_version": "v1.0.0",
    "hyperparameters": {
        "regParam":        lr_model.getRegParam(),
        "elasticNetParam": lr_model.getElasticNetParam(),
        "maxIter":         lr_model.getMaxIter(),
        "numFeatures":     NUM_FEATURES,
    },
}
with open(META_PATH, "w") as f:
    json.dump(meta, f, indent=2)
print(f"✓ Training metadata saved → {META_PATH}")

spark.stop()
print("✓ SparkSession stopped. Training job complete.")
