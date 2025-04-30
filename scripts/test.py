from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from collections import defaultdict
from typing import List, Tuple
import time

from config import (
    KEY_COLUMN,
    COUNT_COLUMN,
    NUM_PARTITIONS,
    SPARK_MASTER_URL,
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
)


start_time = time.time()

spark = SparkSession.builder \
    .appName("RDDPartitionedJoinWithDynamicRanges") \
    .config("spark.sql.shuffle.partitions", "6") \
    .getOrCreate()

sc = spark.sparkContext

# ==========================
# STEP 1: Compute key_ranges
# ==========================

def compute_key_ranges() -> List[Tuple[int, int, int]]:
    # Read keys from both inputs as DataFrame
    left_df = spark.read.option("header", True).csv(HDFS_LEFT_INPUT_PATH).select(col("key").cast("int").alias("key"))
    right_df = spark.read.option("header", True).csv(HDFS_RIGHT_INPUT_PATH).select(col("key").cast("int").alias("key"))

    both_keys_df = left_df.union(right_df)

    key_freqs = (
        both_keys_df.groupBy("key")
        .count()
        .withColumnRenamed("count", "global_count")
        .orderBy("key")
    )

    total = key_freqs.agg(spark_sum("global_count")).collect()[0][0]
    ideal_per_partition = total // NUM_PARTITIONS

    rows = key_freqs.collect()
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

key_ranges = compute_key_ranges()
print("[✔] Computed key ranges:", key_ranges)

# Broadcast ranges for use in RDD logic
range_broadcast = sc.broadcast(key_ranges)

# ==========================
# STEP 2: Load Data as RDDs
# ==========================

def parse_left(line):
    fields = line.split(",")
    return int(fields[1]), ("left", fields[0], fields[2])  # (key, ("left", row_id, payload))

def parse_right(line):
    fields = line.split(",")
    return int(fields[1]), ("right", fields[0], fields[2])  # (key, ("right", row_id, payload))

left_rdd = (
    sc.textFile(HDFS_LEFT_INPUT_PATH)
    .filter(lambda line: not line.startswith("left_row_id"))
    .map(parse_left)
)

right_rdd = (
    sc.textFile(HDFS_RIGHT_INPUT_PATH)
    .filter(lambda line: not line.startswith("right_row_id"))
    .map(parse_right)
)

# ==========================
# STEP 3: Partition-Aware Tagging
# ==========================

def get_partition_id(key: int, ranges: List[Tuple[int, int, int]]) -> int:
    for pid, start, end in ranges:
        if start <= key <= end:
            return pid
    return -1  # fallback

def tag_partition(record):
    key, data = record
    pid = get_partition_id(key, range_broadcast.value)
    return (pid, (key, data))

tagged_rdd = left_rdd.union(right_rdd).map(tag_partition)

# ==========================
# STEP 4: Partition and Join
# ==========================

partitioned = tagged_rdd.partitionBy(NUM_PARTITIONS, lambda pid: pid)

def merge_partition(partition_iter):
    left_map = defaultdict(list)
    right_map = defaultdict(list)
    result = []

    for pid_key, (key, (side, row_id, payload)) in partition_iter:
        if side == "left":
            left_map[key].append((row_id, payload))
        else:
            right_map[key].append((row_id, payload))

    for key in left_map.keys() & right_map.keys():
        for lrow in left_map[key]:
            for rrow in right_map[key]:
                result.append((key, lrow[0], lrow[1], rrow[0], rrow[1]))

    return iter(result)

joined_rdd = partitioned.mapPartitions(merge_partition)

# ==========================
# STEP 5: Save Output
# ==========================
OUTPUT_PATH = "hdfs://nn:9000/data/exp/merged_rdd_output"

joined_rdd.map(lambda row: ",".join(map(str, row))).saveAsTextFile(OUTPUT_PATH)

spark.stop()
print(f"[✔] Join complete. Results saved to {OUTPUT_PATH}")
print(f"[⏱] Total time: {time.time() - start_time:.2f} seconds")

