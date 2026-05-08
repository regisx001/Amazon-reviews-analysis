"""
Spark Evaluation Job
====================
Evaluates the trained LogisticRegression model on the test set.

Reads:
  - data/test_feat (parquet format, pre-processed)
  - data/models/best_lr (fitted model)
  - data/models/training_meta.json (for trained_at timestamp)

Writes:
  - data/model_insights.json (metrics and confusion matrix)
"""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col

# ── CONFIG ────────────────────────────────────────────────────────────────────
DATA_DIR    = "/opt/spark/work-dir/data"
TEST_FEAT   = os.path.join(DATA_DIR, "test_feat")
MODEL_DIR   = os.path.join(DATA_DIR, "models", "best_lr")
META_PATH   = os.path.join(DATA_DIR, "models", "training_meta.json")
INSIGHTS    = os.path.join(DATA_DIR, "model_insights.json")
# ─────────────────────────────────────────────────────────────────────────────

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("AmazonReviews-ModelEvaluation")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"✓ Spark {spark.version} ready.")

# ── 1. Load Data and Model ────────────────────────────────────────────────────
print(f"Loading test features from {TEST_FEAT}...")
test_feat = spark.read.parquet(TEST_FEAT)
total_rows = test_feat.count()
print(f"Test rows: {total_rows:,}")

print(f"Loading model from {MODEL_DIR}...")
model = LogisticRegressionModel.load(MODEL_DIR)

# ── 2. Run Predictions ────────────────────────────────────────────────────────
print("Generating predictions on test set...")
test_preds = model.transform(test_feat)
test_preds.cache()

# ── 3. Calculate Metrics ──────────────────────────────────────────────────────
def ev(preds, metric, label=None):
    kwargs = dict(predictionCol='prediction', labelCol='label', metricName=metric)
    if label is not None:
        kwargs['metricLabel'] = label
    return MulticlassClassificationEvaluator(**kwargs).evaluate(preds)

print("Calculating metrics...")
f1_weighted  = ev(test_preds, 'f1')
accuracy     = ev(test_preds, 'accuracy')
precision_wt = ev(test_preds, 'weightedPrecision')
recall_wt    = ev(test_preds, 'weightedRecall')

print(f"  Weighted F1 : {f1_weighted:.4f}")
print(f"  Accuracy    : {accuracy:.4f}")

per_class = {}
for i, cls in enumerate(["negative", "neutral", "positive"]):
    p = ev(test_preds, 'precisionByLabel', i)
    r = ev(test_preds, 'recallByLabel', i)
    f = ev(test_preds, 'fMeasureByLabel', i)
    per_class[cls] = {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f, 4)}
    print(f"  {cls:>8s}  P:{p:.4f}  R:{r:.4f}  F1:{f:.4f}")

# ── 4. Confusion Matrix ───────────────────────────────────────────────────────
print("Calculating confusion matrix...")
label_and_pred = (
    test_preds
    .select(col("label").cast("int"), col("prediction").cast("int"))
    .collect()
)

matrix = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
for row in label_and_pred:
    true_lbl = int(row["label"])
    pred_lbl = int(row["prediction"])
    if 0 <= true_lbl <= 2 and 0 <= pred_lbl <= 2:
        matrix[true_lbl][pred_lbl] += 1

# ── 5. Write model_insights.json ──────────────────────────────────────────────
now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Read training metadata if available
trained_at = "Unknown"
if os.path.exists(META_PATH):
    try:
        with open(META_PATH, "r") as f:
            meta = json.load(f)
            trained_at = meta.get("trained_at", "Unknown")
    except Exception as e:
        print(f"Warning: Could not read training metadata: {e}")

insights = {
    "evaluated_at": now_iso,
    "model_name": "LogisticRegression",
    "model_version": "v1.0.0",
    "trained_at": trained_at,
    "dataset": f"test_feat (10% split)",
    "metrics": {
        "accuracy": round(accuracy, 4),
        "f1_weighted": round(f1_weighted, 4),
        "precision_weighted": round(precision_wt, 4),
        "recall_weighted": round(recall_wt, 4),
        "per_class": per_class
    },
    "confusion_matrix": {
        "labels": ["negative", "neutral", "positive"],
        "matrix": matrix
    },
    "notes": f"Final evaluation on 10% test split ({total_rows:,} rows)"
}

os.makedirs(os.path.dirname(INSIGHTS), exist_ok=True)
with open(INSIGHTS, "w") as f:
    json.dump(insights, f, indent=4)
print(f"\n✓ model_insights.json written → {INSIGHTS}")

spark.stop()
print("✓ SparkSession stopped. Evaluation job complete.")
