"""
Spark Training Job — Logistic Regression (Best Model)
======================================================
Mirrors the training portion of notebook 04-train-logistic-regresion-model.ipynb.

Pipeline:
  1. Load the train split (CSV)
  2. Prepare data (feature fusion: Summary×2 + Text, numeric label)
  3. Apply custom class weights (negative=3.0, neutral=8.0, positive=0.5)
  4. NLP preprocessing: RegexTokenizer → StopWordsRemover → Lemmatizer
     → NGram bigrams → UnigramBigramCombiner → HashingTF → IDF
  5. Hyperparameter tuning via TrainValidationSplit (12 combos, internal 80/20 split)
  6. Save model artefacts:
       data/models/best_lr/       ← fitted LogisticRegressionModel
       data/models/hashing_tf/    ← fitted HashingTFModel
       data/models/idf/           ← fitted IDFModel

NOTE: Evaluation on the test split (and writing model_insights.json) is done
by a separate Spark evaluation job, not here.
"""

import json
import os
import time
from datetime import datetime, timezone

import nltk

# Point to the NLTK data baked into the Docker image at build time (as root).
# /opt/nltk_data is world-readable and available on every driver/executor.
_NLTK_DATA_DIR = "/opt/nltk_data"
os.environ["NLTK_DATA"] = _NLTK_DATA_DIR
nltk.data.path.insert(0, _NLTK_DATA_DIR)

from nltk.stem import WordNetLemmatizer
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, NGram, RegexTokenizer, StopWordsRemover
from pyspark.ml.param.shared import Param, Params
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, udf, when
from pyspark.sql.types import ArrayType, DoubleType, StringType

# ── CONFIG ────────────────────────────────────────────────────────────────────
DATA_DIR    = "/opt/spark/work-dir/data"
TRAIN_DIR   = os.path.join(DATA_DIR, "train")
MODEL_DIR   = os.path.join(DATA_DIR, "models", "best_lr")
HASHING_DIR = os.path.join(DATA_DIR, "models", "hashing_tf")
IDF_DIR     = os.path.join(DATA_DIR, "models", "idf")

NUM_FEATURES = 100_000
RANDOM_SEED  = 42

WEIGHT_MAP = {
    0: 3.0,   # negative
    1: 8.0,   # neutral  ← pushed hard
    2: 0.5,   # positive ← reduced dominance
}

NEGATION_WORDS = {
    "not", "no", "nor", "never", "neither", "none", "nobody",
    "nowhere", "hardly", "barely", "scarcely",
}
# ─────────────────────────────────────────────────────────────────────────────


# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("AmazonReviews-BestLR-Training")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"✓ Spark {spark.version} ready.")


# ── 1. Load & prepare data ────────────────────────────────────────────────────
def prepare(df):
    df = df.na.drop(subset=["Text"])
    df = df.withColumn("Text",    col("Text").cast("string"))
    df = df.withColumn("Summary", col("Summary").cast("string"))
    df = df.fillna({"Summary": ""})
    # Feature fusion: Summary × 2 + Text (boosts dense summary signal)
    df = df.withColumn(
        "Text",
        concat_ws(" ", col("Summary"), col("Summary"), col("Text")),
    )
    # Numeric label: 0=negative, 1=neutral, 2=positive
    df = df.withColumn(
        "label",
        when(col("Sentiment") == "negative", 0)
        .when(col("Sentiment") == "neutral",  1)
        .otherwise(2),
    )
    return df


print("Loading train split…")
train_df = spark.read.csv(TRAIN_DIR, header=True, inferSchema=True)
train_df = prepare(train_df)
print(f"Train: {train_df.count():,} rows")


# ── 2. Class weights ──────────────────────────────────────────────────────────
def add_weights(df, wmap):
    expr = None
    for lbl, w in wmap.items():
        cond = when(col("label") == lbl, float(w))
        expr = cond if expr is None else expr.when(col("label") == lbl, float(w))
    return df.withColumn("classWeight", expr.otherwise(1.0))


train_df = add_weights(train_df, WEIGHT_MAP)
print("Class weights applied:", {0: "negative=3.0", 1: "neutral=8.0", 2: "positive=0.5"})


# ── 3. NLP Pipeline ──────────────────────────────────────────────────────────
custom_stopwords = [
    w for w in StopWordsRemover.loadDefaultStopWords("english")
    if w not in NEGATION_WORDS
]

_lemmatizer = WordNetLemmatizer()


def lemmatize_tokens(tokens):
    """Lemmatize tokens. Self-contained so executor workers find wordnet data."""
    import os as _os
    import nltk as _nltk
    _nltk.data.path.insert(0, "/opt/nltk_data")
    _os.environ.setdefault("NLTK_DATA", "/opt/nltk_data")
    from nltk.stem import WordNetLemmatizer as _WNL
    _lem = _WNL()
    if tokens is None:
        return []
    result = []
    for w in tokens:
        lv = _lem.lemmatize(w, pos="v")
        ln = _lem.lemmatize(w, pos="n")
        result.append(lv if lv != w else (ln if ln != w else w.lower()))
    return result


class LemmatizerTransformer(Transformer):
    inputCol  = Param(Params._dummy(), "inputCol",  "Input column")
    outputCol = Param(Params._dummy(), "outputCol", "Output column")

    def __init__(self, inputCol="filtered", outputCol="lemmatized"):
        super().__init__()
        self._setDefault(inputCol="filtered", outputCol="lemmatized")
        self._set(inputCol=inputCol, outputCol=outputCol)

    def _transform(self, dataset):
        ic = self.getOrDefault(self.inputCol)
        oc = self.getOrDefault(self.outputCol)
        _u = udf(lemmatize_tokens, ArrayType(StringType()))
        return dataset.withColumn(oc, _u(col(ic)))


class UnigramBigramCombiner(Transformer):
    unigram_col = Param(Params._dummy(), "unigram_col", "unigram col")
    bigram_col  = Param(Params._dummy(), "bigram_col",  "bigram col")
    outputCol   = Param(Params._dummy(), "outputCol",   "output col")

    def __init__(self, unigram_col="lemmatized", bigram_col="bigrams", outputCol="tokens"):
        super().__init__()
        self._setDefault(unigram_col="lemmatized", bigram_col="bigrams", outputCol="tokens")
        self._set(unigram_col=unigram_col, bigram_col=bigram_col, outputCol=outputCol)

    def _transform(self, dataset):
        uc = self.getOrDefault(self.unigram_col)
        bc = self.getOrDefault(self.bigram_col)
        oc = self.getOrDefault(self.outputCol)
        _u = udf(lambda a, b: (a or []) + (b or []), ArrayType(StringType()))
        return dataset.withColumn(oc, _u(col(uc), col(bc)))


tokenizer  = RegexTokenizer(inputCol="Text", outputCol="words", pattern=r"\w+", gaps=False, minTokenLength=2)
remover    = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=custom_stopwords)
lemma_t    = LemmatizerTransformer(inputCol="filtered", outputCol="lemmatized")
bigram     = NGram(n=2, inputCol="lemmatized", outputCol="bigrams")
combiner   = UnigramBigramCombiner()
hashing_tf = HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=NUM_FEATURES)
idf        = IDF(inputCol="raw_features", outputCol="features", minDocFreq=3)

preproc_pipeline = Pipeline(stages=[tokenizer, remover, lemma_t, bigram, combiner, hashing_tf, idf])

print("Fitting preprocessor on train set…")
t0 = time.time()
preprocessor = preproc_pipeline.fit(train_df)
print(f"✓ Preprocessor fitted in {time.time() - t0:.1f}s")

KEEP = ["label", "classWeight", "features"]
train_feat = preprocessor.transform(train_df).select(KEEP).cache()
print(f"Train features: {train_feat.count():,} rows")


# ── 4. Train LogisticRegression ───────────────────────────────────────────────
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


# ── 5. Save model artefacts ───────────────────────────────────────────────────
os.makedirs(os.path.join(DATA_DIR, "models"), exist_ok=True)

lr_model.write().overwrite().save(MODEL_DIR)
print(f"\n✓ LR model saved → {MODEL_DIR}")

fitted_hashing_tf = preprocessor.stages[5]
fitted_idf        = preprocessor.stages[6]
fitted_hashing_tf.write().overwrite().save(HASHING_DIR)
fitted_idf.write().overwrite().save(IDF_DIR)
print(f"✓ HashingTF saved → {HASHING_DIR}")
print(f"✓ IDF saved       → {IDF_DIR}")

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
        "class_weights":   {str(k): v for k, v in WEIGHT_MAP.items()},
    },
}
meta_path = os.path.join(DATA_DIR, "models", "training_meta.json")
with open(meta_path, "w") as f:
    json.dump(meta, f, indent=2)
print(f"✓ Training metadata saved → {meta_path}")

spark.stop()
print("✓ SparkSession stopped. Training job complete.")

