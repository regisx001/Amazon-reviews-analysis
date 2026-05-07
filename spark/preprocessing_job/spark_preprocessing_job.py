import os
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, rand

# ---------- CONFIG ----------
DATA_DIR = "/opt/spark/work-dir/data"
INPUT_FILE = os.path.join(DATA_DIR, "reviews.csv")
TRAIN_DIR = os.path.join(DATA_DIR, "train")
VAL_DIR = os.path.join(DATA_DIR, "val")
TEST_DIR = os.path.join(DATA_DIR, "test")
RANDOM_SEED = 42
# ----------------------------

spark = SparkSession.builder \
    .appName("Preprocessing-Sentiment-Split") \
    .getOrCreate()

# 1. Read CSV
print("Loading reviews.csv...")
df = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)
total = df.count()
print(f"Total rows: {total}")

# 2. Create Sentiment column
df = df.withColumn(
    "Sentiment",
    when(col("Score") < 3, "negative")
    .when(col("Score") == 3, "neutral")
    .otherwise("positive")
)
print("Sentiment distribution:")
df.groupBy("Sentiment").count().show()

# 3. Stratified split (80/10/10)


def stratified_split(df, label_col, train_frac, val_frac, seed):
    """
    Return (train, val, test) DataFrames with class proportions preserved.
    """
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

    # FIX: use reduce instead of unionByName(*list)
    train_all = reduce(lambda a, b: a.unionByName(b), train_dfs)
    val_all = reduce(lambda a, b: a.unionByName(b), val_dfs)
    test_all = reduce(lambda a, b: a.unionByName(b), test_dfs)

    return train_all, val_all, test_all


train, val, test = stratified_split(
    df, "Sentiment", 0.8, 0.1, RANDOM_SEED
)

print(f"Train count:      {train.count()}")
print(f"Validation count: {val.count()}")
print(f"Test count:       {test.count()}")

# 4. Save splits as single CSV file per folder (coalesce(1))
train.coalesce(1).write.csv(TRAIN_DIR, mode="overwrite", header=True)
val.coalesce(1).write.csv(VAL_DIR,   mode="overwrite", header=True)
test.coalesce(1).write.csv(TEST_DIR,  mode="overwrite", header=True)

print("CSV splits saved under:")
print(f"  {TRAIN_DIR}")
print(f"  {VAL_DIR}")
print(f"  {TEST_DIR}")

spark.stop()
