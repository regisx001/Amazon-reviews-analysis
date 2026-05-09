#!/usr/bin/env python3
"""
streaming_job.py — Amazon Reviews Real-Time Sentiment Prediction
"""

from nltk.stem import WordNetLemmatizer
import nltk
import json
import os

import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_unixtime, from_json
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param, Params
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, NGram, HashingTF, IDF, IDFModel
from pyspark.ml.classification import LogisticRegressionModel
# correct import for VectorUDT / Vectors
from pyspark.ml.linalg import Vectors

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
except ImportError:
    KafkaAdminClient = NewTopic = TopicAlreadyExistsError = None


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",            "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC",                "reviews.raw")
MONGO_DB = os.getenv("MONGODB_DATABASE",           "amazon_reviews")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION",           "predictions")
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "1000"))

BASE_DIR = os.getenv("BASE_DIR", "/opt/spark/work-dir/data")
MODEL_DIR = os.path.join(BASE_DIR, "models")
LR_PATH = os.path.join(MODEL_DIR, "best_lr")
IDF_PATH = os.path.join(MODEL_DIR, "idf")

NUM_FEATURES = 100_000
MIN_DOC_FREQ = 3

MONGO_URI = os.getenv("MONGO_URI") or (
    "mongodb://{user}:{pw}@{host}:{port}/".format(
        user=os.getenv("MONGODB_ROOT_USER",     "admin"),
        pw=os.getenv("MONGODB_ROOT_PASSWORD", "admin123"),
        host=os.getenv("MONGODB_HOST",          "mongodb"),
        port=os.getenv("MONGODB_PORT",          "27017"),
    )
)

LABEL_MAP = {0: "negative", 1: "neutral", 2: "positive"}


# -----------------------------------------------------------------
# Kafka topic helper
# -----------------------------------------------------------------
def ensure_kafka_topic():
    if KafkaAdminClient is None:
        print("[WARN] kafka-python not available — skipping topic creation.")
        return
    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP, client_id="spark-init")
        if KAFKA_TOPIC in admin.list_topics():
            print(f"[INFO] Topic '{KAFKA_TOPIC}' already exists.")
            return
        admin.create_topics([NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1")),
            replication_factor=int(os.getenv("KAFKA_TOPIC_REPLICATION", "1")),
        )], validate_only=False)
        print(f"[INFO] Created topic: {KAFKA_TOPIC}")
    except Exception as exc:
        if not (TopicAlreadyExistsError and isinstance(exc, TopicAlreadyExistsError)):
            print(f"[WARN] Kafka topic check: {exc}")
    finally:
        if admin:
            admin.close()


# -----------------------------------------------------------------
# SparkSession
# -----------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AmazonReviews-StreamingPredictions")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"[INFO] Spark {spark.version} ready.")


# -----------------------------------------------------------------
# NLP setup — NLTK with offline fallback
# wordnet / omw-1.4 must be pre-installed in the Docker image.
# If download fails (no internet) we still continue — the data
# should already be present in /usr/share/nltk_data inside the image.
# -----------------------------------------------------------------
nltk.data.path.append("/usr/share/nltk_data")   # standard Docker path

try:
    nltk.download("wordnet", quiet=True, raise_on_error=True)
    nltk.download("omw-1.4",  quiet=True, raise_on_error=True)
    print("[INFO] NLTK data downloaded successfully.")
except Exception:
    print("[INFO] NLTK download skipped (no internet) — using pre-installed data.")

_lemmatizer = WordNetLemmatizer()

NEGATION_WORDS = {
    "not", "no", "nor", "never", "neither", "none", "nobody",
    "nowhere", "hardly", "barely", "scarcely",
}
CUSTOM_SW = [w for w in StopWordsRemover.loadDefaultStopWords("english")
             if w not in NEGATION_WORDS]


def _lemmatize_tokens(tokens):
    if not tokens:
        return []
    out = []
    for w in tokens:
        lv = _lemmatizer.lemmatize(w, pos="v")
        ln = _lemmatizer.lemmatize(w, pos="n")
        out.append(lv if lv != w else (ln if ln != w else w.lower()))
    return out


# -----------------------------------------------------------------
# Custom Transformers — verbatim from notebook 02
# -----------------------------------------------------------------
class LemmatizerTransformer(Transformer):
    inputCol = Param(Params._dummy(), "inputCol",  "Input column")
    outputCol = Param(Params._dummy(), "outputCol", "Output column")

    def __init__(self, inputCol="filtered", outputCol="lemmatized"):
        super().__init__()
        self._setDefault(inputCol="filtered", outputCol="lemmatized")
        self._set(inputCol=inputCol, outputCol=outputCol)

    def setInputCol(self,  v): return self._set(inputCol=v)
    def setOutputCol(self, v): return self._set(outputCol=v)

    def _transform(self, dataset):
        ic = self.getOrDefault(self.inputCol)
        oc = self.getOrDefault(self.outputCol)
        _u = udf(_lemmatize_tokens, ArrayType(StringType()))
        return dataset.withColumn(oc, _u(col(ic)))


class UnigramBigramCombiner(Transformer):
    unigram_col = Param(Params._dummy(), "unigram_col", "unigram column")
    bigram_col = Param(Params._dummy(), "bigram_col",  "bigram column")
    outputCol = Param(Params._dummy(), "outputCol",   "output column")

    def __init__(self, unigram_col="lemmatized", bigram_col="bigrams", outputCol="tokens"):
        super().__init__()
        self._setDefault(unigram_col="lemmatized",
                         bigram_col="bigrams", outputCol="tokens")
        self._set(unigram_col=unigram_col,
                  bigram_col=bigram_col, outputCol=outputCol)

    def _transform(self, dataset):
        uc = self.getOrDefault(self.unigram_col)
        bc = self.getOrDefault(self.bigram_col)
        oc = self.getOrDefault(self.outputCol)
        _u = udf(lambda a, b: (a or []) + (b or []), ArrayType(StringType()))
        return dataset.withColumn(oc, _u(col(uc), col(bc)))


# -----------------------------------------------------------------
# Load LogisticRegressionModel
# -----------------------------------------------------------------
print("[INFO] Loading LogisticRegressionModel...")
try:
    best_lr = LogisticRegressionModel.load(LR_PATH)
    print(f"  [OK] LR model       <- {LR_PATH}")
    print(f"       featuresCol    : {best_lr.getFeaturesCol()}")
except Exception as e:
    print(f"[ERROR] LR model load failed: {e}")
    spark.stop()
    raise


# -----------------------------------------------------------------
# Load IDFModel via dummy-fit + _java_obj patch
# Fix: use pyspark.ml.linalg.Vectors (not pyspark.sql.types.VectorUDT)
# -----------------------------------------------------------------
print("[INFO] Loading IDFModel...")
fitted_idf = None
try:
    fitted_idf = IDFModel.load(IDF_PATH)
    print(f"  [OK] IDFModel       <- {IDF_PATH}")
except Exception as e:
    print(f"[WARN] IDFModel load failed: {e}")
    try:
        dummy_data = spark.createDataFrame(
            [(Vectors.sparse(NUM_FEATURES, [0], [1.0]),),
             (Vectors.sparse(NUM_FEATURES, [1], [1.0]),),
             (Vectors.sparse(NUM_FEATURES, [2], [1.0]),)],
            schema="raw_features vector",   # no VectorUDT import needed
        )
        dummy_idf_model = IDF(
            inputCol="raw_features", outputCol="features", minDocFreq=MIN_DOC_FREQ,
        ).fit(dummy_data)

        saved_java_idf = spark._jvm.org.apache.spark.ml.feature.IDFModel.load(
            IDF_PATH)
        dummy_idf_model._java_obj = saved_java_idf
        fitted_idf = dummy_idf_model
        print(f"  [OK] IDFModel       <- {IDF_PATH}")
    except Exception as e2:
        print(f"[WARN] IDFModel patch failed: {e2}")
        print("[WARN] Falling back to TF-only features. Minor accuracy drop expected.")


# -----------------------------------------------------------------
# Stateless pipeline stages (re-created — identical params to notebook 02)
# -----------------------------------------------------------------
tokenizer = RegexTokenizer(inputCol="Text", outputCol="words",
                           pattern=r"\w+", gaps=False, minTokenLength=2)
remover = StopWordsRemover(inputCol="words", outputCol="filtered",
                           stopWords=CUSTOM_SW)
lemmatizer_t = LemmatizerTransformer(
    inputCol="filtered", outputCol="lemmatized")
bigram = NGram(n=2, inputCol="lemmatized", outputCol="bigrams")
combiner = UnigramBigramCombiner(unigram_col="lemmatized",
                                 bigram_col="bigrams", outputCol="tokens")
hashing_tf = HashingTF(inputCol="tokens", outputCol="raw_features",
                       numFeatures=NUM_FEATURES)


# -----------------------------------------------------------------
# Kafka schema + stream
# -----------------------------------------------------------------
json_schema = StructType([
    StructField("Id",                     StringType(), True),
    StructField("ProductId",              StringType(), True),
    StructField("UserId",                 StringType(), True),
    StructField("ProfileName",            StringType(), True),
    StructField("HelpfulnessNumerator",   StringType(), True),
    StructField("HelpfulnessDenominator", StringType(), True),
    StructField("Score",                  StringType(), True),
    StructField("Time",                   StringType(), True),
    StructField("Summary",                StringType(), True),
    StructField("Text",                   StringType(), True),
])

ensure_kafka_topic()

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "earliest")
    .load()
)

parsed = (
    raw_stream
    .select(from_json(col("value").cast("string"), json_schema).alias("d"))
    .select("d.*")
)


# -----------------------------------------------------------------
# Full preprocessing pipeline
# -----------------------------------------------------------------
df = tokenizer.transform(parsed)
df = remover.transform(df)
df = lemmatizer_t.transform(df)
df = bigram.transform(df)
df = combiner.transform(df)
df = hashing_tf.transform(df)                        # -> raw_features

if fitted_idf is not None:
    df = fitted_idf.transform(df)                    # -> features
else:
    df = df.withColumnRenamed("raw_features", "features")

predictions_df = best_lr.transform(df)


# -----------------------------------------------------------------
# Post-processing
# -----------------------------------------------------------------
label_udf = udf(
    lambda x: LABEL_MAP.get(int(x), "unknown") if x is not None else "unknown",
    StringType(),
)

predictions_df = (
    predictions_df
    .withColumn("PredictedSentiment", label_udf(col("prediction")))
    .withColumn("ProcessingTime",     from_unixtime(col("Time").cast("long")))
    .withColumn("Score",              col("Score").cast("double"))
    .withColumn("Time",               col("Time").cast("long"))
    .select(
        "Id", "ProductId", "UserId", "ProfileName",
        "Score", "Time", "ProcessingTime",
        "Summary", "Text",
        "prediction", "PredictedSentiment",
    )
)


# -----------------------------------------------------------------
# MongoDB writer
# -----------------------------------------------------------------
def write_to_mongo(batch_df, batch_id):
    client = pymongo.MongoClient(MONGO_URI)
    try:
        buffer = []
        inserted = 0
        for row_json in batch_df.toJSON().toLocalIterator():
            buffer.append(json.loads(row_json))
            if len(buffer) >= MONGO_BATCH_SIZE:
                client[MONGO_DB][MONGO_COLLECTION].insert_many(buffer)
                inserted += len(buffer)
                buffer = []
        if buffer:
            client[MONGO_DB][MONGO_COLLECTION].insert_many(buffer)
            inserted += len(buffer)
        if inserted == 0:
            print(f"[Batch {batch_id}] Empty, skipping.")
        else:
            print(
                f"[Batch {batch_id}] Inserted {inserted} docs -> {MONGO_DB}.{MONGO_COLLECTION}")
    finally:
        client.close()


# -----------------------------------------------------------------
# Start streaming query
# -----------------------------------------------------------------
query = (
    predictions_df.writeStream
    .foreachBatch(write_to_mongo)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/tmp/spark-checkpoints/reviews-stream")
    .start()
)

print(f"[INFO] Streaming query started. Listening on topic: {KAFKA_TOPIC}")
query.awaitTermination()
