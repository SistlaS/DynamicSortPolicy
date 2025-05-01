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
left_with_pid_df = left_with_pid_rdd.map(lambda d: Row(**d)).toDF()

right_with_pid_rdd = right_rdd.map(lambda row: row.asDict()) \
    .map(lambda d: assign_pid_from_dict(d, broadcast_pid_map.value))
# right_with_pid_rdd = right_rdd.map(lambda row: row.asDict()) \
#     .map(lambda d: {**d, "pid": get_partition_pid(int(d[KEY_COLUMN]), broadcast_partitions.value)})
right_with_pid_df = right_with_pid_rdd.map(lambda d: Row(**d)).toDF()

print(f"*******************Partitions: {NUM_PARTITIONS}********************")

left_count_before = left_with_pid_df.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()
right_count_before = right_with_pid_df.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()

print(f"Left count before repartitioning: {left_count_before}")
print(f"Right count before repartitioning: {right_count_before}")

left_df_partitioned = left_with_pid_df.repartition(NUM_PARTITIONS, int(col("pid")))
right_df_partitioned = right_with_pid_df.repartition(NUM_PARTITIONS, int(col("pid")))

left_count_after = left_df_partitioned.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()
right_count_after = right_df_partitioned.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()

print(f"Left count after repartitioning: {left_count_after}")
print(f"Right count after repartitioning: {right_count_after}")


def print_partition_stats(index, iterator):
    keys = set()
    row_count = 0
    for row in iterator:
        keys.add(row["key"])
        row_count += 1
    print(f"[Partition {index}] Row count: {row_count}, Distinct keys: {len(keys)}")
    if '1' in keys:
        print(f"[Partition {index}] Found key 1")
    print(keys)
    return []
# === Step 7: Perform Join and Write Output ===

joined_df = left_with_pid_df.join(
    right_with_pid_df,
    on=["pid", "key"],
    how="inner"
)

# left_with_pid_df.foreachPartition(lambda it: print_partition_stats(0, it))
left_df_partitioned.select("key").rdd.mapPartitionsWithIndex(print_partition_stats).collect()
right_df_partitioned.select("key").rdd.mapPartitionsWithIndex(print_partition_stats).collect()
# right_with_pid_df.foreachPartition(lambda it: print_partition_stats(1, it))
joined_df.select("key").rdd.mapPartitionsWithIndex(print_partition_stats).collect()


counts_perpartition = left_with_pid_df.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()

print(counts_per_partition)
# output_path = "/tmp/partition_counts.txt"  # You can change this to HDFS if needed

# # === Compute counts ===
# left_key_distinct_per_partition = (
#     left_with_pid_df.select("key").rdd
#     .mapPartitionsWithIndex(
#         lambda idx, it: [(idx, len(set(row["key"] for row in it)))],  # distinct keys
#         preservesPartitioning=True
#     )
#     .collect()
# )

# right_key_distinct_per_partition = (
#     right_with_pid_df.select("key").rdd
#     .mapPartitionsWithIndex(
#         lambda idx, it: [(idx, len(set(row["key"] for row in it)))],
#         preservesPartitioning=True
#     )
#     .collect()
# )

# joined_key_distinct_per_partition = (
#     joined_df.select("key").rdd
#     .mapPartitionsWithIndex(
#         lambda idx, it: [(idx, len(set(row["key"] for row in it)))],
#         preservesPartitioning=True
#     )
#     .collect()
# )

# # === Write counts to file ===
# with open("/tmp/distinct_keys_per_partition.txt", "w") as f:
#     f.write("=== DISTINCT KEYS PER SPARK PARTITION ===\n\n")

#     f.write("LEFT:\n")
#     for pid, count in sorted(left_key_distinct_per_partition):
#         f.write(f"Partition {pid}: {count} distinct keys\n")

#     f.write("\nRIGHT:\n")
#     for pid, count in sorted(right_key_distinct_per_partition):
#         f.write(f"Partition {pid}: {count} distinct keys\n")

#     f.write("\nJOINED:\n")
#     for pid, count in sorted(joined_key_distinct_per_partition):
#         f.write(f"Partition {pid}: {count} distinct keys\n")



joined_df.write \
    .partitionBy("pid") \
    .option("header", True) \
    .mode("overwrite") \
    .csv("hdfs://nn:9000/data/output_partitioned_join")

# === (Optional) Show Sample ===
