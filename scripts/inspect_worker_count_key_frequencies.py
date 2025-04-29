from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import (
    LOCAL_TMP_WORKER_STATS_DIR,
    KEY_COLUMN,
)

# Create Spark session
spark = (
    SparkSession.builder
    .appName("inspect_worker_count_key_frequencies")
    .master("spark://master:7077")
    .getOrCreate()
)

# Get Application ID (unique worker ID)
worker_id = spark.sparkContext.applicationId.replace("-", "_")

# Input paths (both left and right)
csv_path_left = "hdfs://nn:9000/data/exp-30rows-1000keys-zipf2/left/*.csv"
csv_path_right = "hdfs://nn:9000/data/exp-30rows-1000keys-zipf2/right/*.csv"

# Output directory
output_path = f"hdfs://nn:9000/{LOCAL_TMP_WORKER_STATS_DIR}/worker_{worker_id}"

# Read input CSVs
df_left = spark.read.option("header", True).csv(csv_path_left)
df_right = spark.read.option("header", True).csv(csv_path_right)

# Select only key column
df_left_keys = df_left.select(col(KEY_COLUMN).cast("int").alias("key"))
df_right_keys = df_right.select(col(KEY_COLUMN).cast("int").alias("key"))

# Union both
df_all = df_left_keys.union(df_right_keys)

# Group by key and count
key_freq = (
    df_all.groupBy("key")
    .count()
    .orderBy("count", ascending=False)
)

# Write result
key_freq.coalesce(1).write \
    .option("header", True) \
    .csv(output_path)

print(f"Combined LEFT + RIGHT key frequencies written to {output_path}")

spark.stop()
