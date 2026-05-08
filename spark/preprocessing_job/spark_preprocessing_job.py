"""
Spark Preprocessing Job
=======================
1. Reads reviews.csv.
2. Creates Sentiment labels.
3. Splits into train/val/test (stratified).
4. Applies feature fusion and class weights.
5. Runs NLP pipeline (TF-IDF, lemmatization, bigrams).
6. Saves train_feat, val_feat, test_feat (parquet) and NLP models.
"""

import os
import time
from functools import reduce

import nltk

# Point to the NLTK data baked into the Docker image at build time (as root).
_NLTK_DATA_DIR = "/opt/nltk_data"
os.environ["NLTK_DATA"] = _NLTK_DATA_DIR
nltk.data.path.insert(0, _NLTK_DATA_DIR)

from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import HashingTF, IDF, NGram, RegexTokenizer, StopWordsRemover
from pyspark.ml.param.shared import Param, Params
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, rand, udf, when
from pyspark.sql.types import ArrayType, StringType

# ---------- CONFIG ----------
DATA_DIR    = "/opt/spark/work-dir/data"
INPUT_FILE  = os.path.join(DATA_DIR, "reviews.csv")
TRAIN_DIR   = os.path.join(DATA_DIR, "train")
VAL_DIR     = os.path.join(DATA_DIR, "val")
TEST_DIR    = os.path.join(DATA_DIR, "test")

TRAIN_FEAT  = os.path.join(DATA_DIR, "train_feat")
VAL_FEAT    = os.path.join(DATA_DIR, "val_feat")
TEST_FEAT   = os.path.join(DATA_DIR, "test_feat")

HASHING_DIR = os.path.join(DATA_DIR, "models", "hashing_tf")
IDF_DIR     = os.path.join(DATA_DIR, "models", "idf")

RANDOM_SEED  = 42
NUM_FEATURES = 100_000

WEIGHT_MAP = {
    0: 3.0,   # negative
    1: 8.0,   # neutral
    2: 0.5,   # positive
}

NEGATION_WORDS = {
    "not", "no", "nor", "never", "neither", "none", "nobody",
    "nowhere", "hardly", "barely", "scarcely",
}
# ----------------------------

spark = SparkSession.builder \
    .appName("Preprocessing-NLP-Pipeline") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Read CSV and Create Sentiment column
print("Loading reviews.csv...")
df = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)

df = df.withColumn(
    "Sentiment",
    when(col("Score") < 3, "negative")
    .when(col("Score") == 3, "neutral")
    .otherwise("positive")
)

# 2. Stratified split (80/10/10)
def stratified_split(df, label_col, train_frac, val_frac, seed):
    classes = [row[0] for row in df.select(label_col).distinct().collect()]
    train_dfs, val_dfs, test_dfs = [], [], []

    for cls in classes:
        cls_df = df.filter(col(label_col) == cls).orderBy(rand(seed))
        total = cls_df.count()
        train_cnt = int(total * train_frac)
        val_cnt = int(total * val_frac)

        train = cls_df.limit(train_cnt)
        remainder = cls_df.subtract(train)
        val = remainder.limit(val_cnt)
        test = remainder.subtract(val)

        train_dfs.append(train)
        val_dfs.append(val)
        test_dfs.append(test)

    train_all = reduce(lambda a, b: a.unionByName(b), train_dfs)
    val_all = reduce(lambda a, b: a.unionByName(b), val_dfs)
    test_all = reduce(lambda a, b: a.unionByName(b), test_dfs)

    return train_all, val_all, test_all

print("Splitting data (train/val/test)...")
train, val, test = stratified_split(df, "Sentiment", 0.8, 0.1, RANDOM_SEED)

# (Optional) Save CSV splits just like before
train.coalesce(1).write.csv(TRAIN_DIR, mode="overwrite", header=True)
val.coalesce(1).write.csv(VAL_DIR, mode="overwrite", header=True)
test.coalesce(1).write.csv(TEST_DIR, mode="overwrite", header=True)

# 3. Data Preparation (Feature fusion, numeric labels, weights)
def prepare_and_weight(data, wmap):
    data = data.na.drop(subset=["Text"])
    data = data.withColumn("Text", col("Text").cast("string"))
    data = data.withColumn("Summary", col("Summary").cast("string"))
    data = data.fillna({"Summary": ""})
    
    # Feature fusion
    data = data.withColumn(
        "Text",
        concat_ws(" ", col("Summary"), col("Summary"), col("Text")),
    )
    # Numeric label
    data = data.withColumn(
        "label",
        when(col("Sentiment") == "negative", 0)
        .when(col("Sentiment") == "neutral",  1)
        .otherwise(2),
    )
    
    # Add weights
    expr = None
    for lbl, w in wmap.items():
        cond = when(col("label") == lbl, float(w))
        expr = cond if expr is None else expr.when(col("label") == lbl, float(w))
    data = data.withColumn("classWeight", expr.otherwise(1.0))
    
    return data

train_prep = prepare_and_weight(train, WEIGHT_MAP)
val_prep   = prepare_and_weight(val, WEIGHT_MAP)
test_prep  = prepare_and_weight(test, WEIGHT_MAP)

# 4. NLP Pipeline
custom_stopwords = [
    w for w in StopWordsRemover.loadDefaultStopWords("english")
    if w not in NEGATION_WORDS
]

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

print("Fitting NLP pipeline on train set…")
t0 = time.time()
preprocessor = preproc_pipeline.fit(train_prep)
print(f"✓ Pipeline fitted in {time.time() - t0:.1f}s")

# 5. Transform and Save Features
KEEP = ["label", "classWeight", "features"]

print(f"Saving train_feat to {TRAIN_FEAT}...")
train_feat = preprocessor.transform(train_prep).select(KEEP)
train_feat.write.parquet(TRAIN_FEAT, mode="overwrite")

print(f"Saving val_feat to {VAL_FEAT}...")
val_feat = preprocessor.transform(val_prep).select(KEEP)
val_feat.write.parquet(VAL_FEAT, mode="overwrite")

print(f"Saving test_feat to {TEST_FEAT}...")
test_feat = preprocessor.transform(test_prep).select(KEEP)
test_feat.write.parquet(TEST_FEAT, mode="overwrite")

# 6. Save HashingTF and IDF models
os.makedirs(os.path.join(DATA_DIR, "models"), exist_ok=True)
fitted_hashing_tf = preprocessor.stages[5]
fitted_idf        = preprocessor.stages[6]
fitted_hashing_tf.write().overwrite().save(HASHING_DIR)
fitted_idf.write().overwrite().save(IDF_DIR)
print(f"✓ HashingTF saved → {HASHING_DIR}")
print(f"✓ IDF saved       → {IDF_DIR}")

spark.stop()
print("✓ Preprocessing job complete.")
