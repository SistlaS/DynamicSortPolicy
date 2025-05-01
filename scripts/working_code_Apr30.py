from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import DataFrame
from typing import List, Tuple
import time

# Configuration
from config import (
    EXPERIMENT_NAME,
    KEY_COLUMN,
    NUM_PARTITIONS,
    SPARK_MASTER_URL,
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
)

start = time.time()

# Initialize Spark session
spark = (   
    SparkSession.builder
    .appName(f"Unified Merge Join with foreachPartition - {EXPERIMENT_NAME}")
    .master(SPARK_MASTER_URL)
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.executor.memory", "25g")
    .config("spark.driver.memory", "25g")
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs://nn:9000/spark-logs/") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://nn:9000") \
    .getOrCreate()
)
left_rdd = spark.read.option("header", True).csv(HDFS_LEFT_INPUT_PATH).rdd.cache()
right_rdd = spark.read.option("header", True).csv(HDFS_RIGHT_INPUT_PATH).rdd.cache()

def compute_key_frequencies():
    left_rdd_counts = left_rdd.map(lambda row: int(row[KEY_COLUMN]))
    right_rdd_counts = right_rdd.map(lambda row: int(row[KEY_COLUMN]))
    left_key_freqs = (
        left_rdd.map(lambda row: int(row[KEY_COLUMN]))
                .map(lambda k: (k, 1))
                .reduceByKey(lambda a, b: a + b)
    )
    right_key_freqs = (
        right_rdd.map(lambda row: int(row[KEY_COLUMN]))
                .map(lambda k: (k, 1))
                .reduceByKey(lambda a, b: a + b)
    )
    combined_key_freqs = left_key_freqs.union(right_key_freqs) \
        .reduceByKey(lambda a, b: a + b)
    key_freqs_sorted = combined_key_freqs.sortByKey()
    return key_freqs_sorted


#STEP 2: Compute key ranges
def compute_key_ranges(key_freqs_rdd: DataFrame) -> List[Tuple[int, int, int]]:
    rows = key_freqs_sorted.collect()  # List of (key, global_count)
    total = sum([c for _, c in rows])
    ideal_per_partition = total // NUM_PARTITIONS
    partitions = []
    current_sum = 0
    current_keys = []
    pid = 0
    for k, count in rows:
        current_sum += count
        current_keys.append(k)
        if current_sum >= ideal_per_partition and pid < NUM_PARTITIONS - 1:
            partitions.append((pid, current_keys[0], current_keys[-1]))
            pid += 1
            current_sum = 0
            current_keys = []
    if current_keys:
        partitions.append((pid, current_keys[0], current_keys[-1]))
    return partitions

def compute_key_ranges_v2(key_freqs_sorted) -> dict:
    """
    Assign each key to the partition with the least current load.
    This is effective when key frequencies are highly skewed.
    Returns: List of (pid, start_key, end_key)
    """
    rows = key_freqs_sorted.collect()  # List of (key, count)
    partitions_count = [0] * NUM_PARTITIONS
    # partition_to_keys = {i: [] for i in range(NUM_PARTITIONS)}
    key_to_partition = {}
    # Assign each key to the partition with the least current total
    for k, count in rows:
        min_partition = partitions_count.index(min(partitions_count))
        key_to_partition[k] = min_partition
        partitions_count[min_partition] += count
            # Generate (pid, start_key, end_key) per partition
    return key_to_partition

#Step 4

def get_partition_pid(key: int, partition_list: List[Tuple[int, int, int]]) -> int:
    for pid, start, end in partition_list:
        if start <= key <= end:
            return pid
    return -1

def assign_pid_from_dict(row_dict, key_to_pid):
    key = int(row_dict[KEY_COLUMN])
    pid = key_to_pid.get(key, -1)  # -1 if key not found
    return {**row_dict, "pid": pid}
# =====================================================================
# PIPELINE
# =====================================================================

key_freqs_sorted = compute_key_frequencies()
key_to_pid = compute_key_ranges_v2(key_freqs_sorted)

broadcast_pid_map = spark.sparkContext.broadcast(key_to_pid)

left_with_pid_rdd = left_rdd.map(lambda row: row.asDict()) \
    .map(lambda d: assign_pid_from_dict(d, broadcast_pid_map.value))
# left_with_pid_rdd = left_rdd.map(lambda row: row.asDict()) \
#     .map(lambda d: {**d, "pid": get_partition_pid(int(d[KEY_COLUMN]), broadcast_partitions.value)})
# left_with_pid_df = left_with_pid_rdd.map(lambda d: Row(**d)).toDF()

right_with_pid_rdd = right_rdd.map(lambda row: row.asDict()) \
    .map(lambda d: assign_pid_from_dict(d, broadcast_pid_map.value))
# right_with_pid_rdd = right_rdd.map(lambda row: row.asDict()) \
#     .map(lambda d: {**d, "pid": get_partition_pid(int(d[KEY_COLUMN]), broadcast_partitions.value)})
# right_with_pid_df = right_with_pid_rdd.map(lambda d: Row(**d)).toDF()

# print(f"*******************Partitions: {NUM_PARTITIONS}********************")

# left_count_before = left_with_pid_df.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()
# right_count_before = right_with_pid_df.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()

# print(f"Left count before repartitioning: {left_count_before}")
# print(f"Right count before repartitioning: {right_count_before}")


left_rdd_partitioned = left_with_pid_rdd.map(lambda row: (row["pid"], row)).partitionBy(NUM_PARTITIONS, lambda x: x)
right_rdd_partitioned = right_with_pid_rdd.map(lambda row: (row["pid"], row)).partitionBy(NUM_PARTITIONS, lambda x: x)
# left_df_partitioned = left_with_pid_df.repartition(NUM_PARTITIONS, int(col("pid")))
# right_df_partitioned = right_with_pid_df.repartition(NUM_PARTITIONS, int(col("pid")))

tagged_left = left_rdd_partitioned.mapValues(lambda row: ("left", row))
tagged_right = right_rdd_partitioned.mapValues(lambda row: ("right", row))

unioned = tagged_left.union(tagged_right)

# === Step 3: Group by pid (local only) ===
grouped_by_pid = unioned.groupByKey()

# === Step 4: Join within each partition ===
def join_within_partition(rows):
    from collections import defaultdict
    left_map = defaultdict(list)
    right_map = defaultdict(list)
    for tag, row in rows:
        if tag == "left":
            left_map[row["key"]].append(row)
        else:
            right_map[row["key"]].append(row)
    for key in left_map:
        if key in right_map:
            for l in left_map[key]:
                for r in right_map[key]:
                    yield {**l, **r}  # Merge the two dicts

joined_rdd = grouped_by_pid.flatMapValues(join_within_partition)  # (pid, merged_row_dict)

# === Step 5: Convert back to DataFrame ===
# You can keep pid as a column or drop it
final_df = joined_rdd.map(lambda x: Row(**x[1])).toDF()


final_df.write \
    .partitionBy("pid") \
    .option("header", True) \
    .mode("overwrite") \
    .csv("hdfs://nn:9000/data/output_partitioned_join")

# === (Optional) Show Sample ===
