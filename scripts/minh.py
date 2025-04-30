from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# ===================
# Configuration
# ===================
from config import (
    EXPERIMENT_NAME,
    KEY_COLUMN,
    COUNT_COLUMN,
    NUM_PARTITIONS,
    SPARK_MASTER_URL,
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
)

start_time = time.time()

NUM_PARTITIONS = 3
spark = SparkSession.builder \
    .appName("Baseline_RangeSortMerge") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .config("spark.sql.shuffle.partitions", str(NUM_PARTITIONS)) \
    .getOrCreate()

# Load data
left_df = spark.read.csv(HDFS_LEFT_INPUT_PATH, header=True, inferSchema=True)
right_df = spark.read.csv(HDFS_RIGHT_INPUT_PATH, header=True, inferSchema=True)

# === Repartition both sides using range partitioning on the key ===
left_df = left_df.repartitionByRange(NUM_PARTITIONS, col(KEY_COLUMN)).sortWithinPartitions(KEY_COLUMN)
right_df = right_df.repartitionByRange(NUM_PARTITIONS, col(KEY_COLUMN)).sortWithinPartitions(KEY_COLUMN)

# === Perform sort-merge join ===
joined = left_df.join(right_df, on=KEY_COLUMN, how="inner")

# Save output
joined.write.mode("overwrite").csv("hdfs://nn:9000/output/sortmerge_range/")

end_time = time.time()
print(f"Total Runtime: {end_time - start_time:.2f} seconds")
