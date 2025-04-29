from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, broadcast
from pyspark.sql import DataFrame
from typing import List, Tuple
import time

# ===================
# Configuration
# ===================
from config import (
    KEY_COLUMN,
    COUNT_COLUMN,
    NUM_PARTITIONS,
    SPARK_MASTER_URL,
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
)

# ===================
# End-to-End Spark Job: Plan + Shuffle + Join
# ===================
start = time.time()

spark = (
    SparkSession.builder
    .appName("Unified Partitioned Merge Join")
    .master(SPARK_MASTER_URL)
    .getOrCreate()
)

# Step 1: Read and Count Key Frequencies from Both Tables
def compute_key_frequencies() -> DataFrame:
    left_df = spark.read.option("header", True).csv(HDFS_LEFT_INPUT_PATH)
    right_df = spark.read.option("header", True).csv(HDFS_RIGHT_INPUT_PATH)

    both_df = left_df.select(KEY_COLUMN).union(right_df.select(KEY_COLUMN))
    both_df = both_df.select(col(KEY_COLUMN).cast("int").alias("key"))

    key_freqs = (
        both_df.groupBy("key")
        .count()
        .withColumnRenamed("count", "global_count")
        .orderBy("key")
    )

    return key_freqs

# Step 2: Compute Key Ranges
def compute_key_ranges(key_freqs_df: DataFrame) -> List[Tuple[int, int, int]]:
    total = key_freqs_df.agg(spark_sum("global_count")).collect()[0][0]
    ideal_per_partition = total // NUM_PARTITIONS

    rows = key_freqs_df.collect()
    partitions = []
    current_sum = 0
    current_keys = []
    pid = 0

    for row in rows:
        k = row["key"]
        c = row["global_count"]
        current_sum += c
        current_keys.append(k)

        if current_sum >= ideal_per_partition and pid < NUM_PARTITIONS - 1:
            partitions.append((pid, current_keys[0], current_keys[-1]))
            pid += 1
            current_sum = 0
            current_keys = []

    if current_keys:
        partitions.append((pid, current_keys[0], current_keys[-1]))

    return partitions

# Step 3: Filter, Join, and Write Per Partition
# Instead of merging DataFrames in driver, write each partition's join output directly
def filter_join_and_write(partitions: List[Tuple[int, int, int]], output_base: str) -> None:
    left_df = spark.read.option("header", True).csv(HDFS_LEFT_INPUT_PATH)
    right_df = spark.read.option("header", True).csv(HDFS_RIGHT_INPUT_PATH)

    left_df = left_df.select(col("left_row_id"), col(KEY_COLUMN).cast("int"), col("left_payload"))
    right_df = right_df.select(col("right_row_id"), col(KEY_COLUMN).cast("int"), col("right_payload"))

    for pid, start_k, end_k in partitions:
        print(f"Processing partition {pid}: keys {start_k} to {end_k}")
        left_part = left_df.filter((col(KEY_COLUMN) >= start_k) & (col(KEY_COLUMN) <= end_k))
        right_part = right_df.filter((col(KEY_COLUMN) >= start_k) & (col(KEY_COLUMN) <= end_k))

        joined = left_part.join(
            right_part,
            on=KEY_COLUMN,
            how="inner"
        )

        # Write each partition out to its own folder
        output_path = f"{output_base}/partition_{pid}"
        joined.write.option("header", True).csv(output_path)
        print(f"Written partition {pid} to {output_path}")

# ==== Run the Pipeline ====
print("[1] Counting key frequencies...")
key_freqs = compute_key_frequencies()

print("[2] Computing key ranges...")
key_ranges = compute_key_ranges(key_freqs)
#broadcast key_ranges
broadcast_keys = spark.sparkContext.broadcast(key_ranges)

print("[3] Filtering, joining, and writing partitions...")
output_base = f"hdfs://nn:9000/data/joined_output"
filter_join_and_write(broadcast_keys.values, output_base)

print(f"[✔] All partitions written under {output_base}")

spark.stop()

end = time.time()

print(f"[✔] Total time taken: {end - start:.2f} seconds")
