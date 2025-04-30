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

left_rdd_counts = left_rdd.map(lambda row: int(row[KEY_COLUMN]))
right_rdd_counts = right_rdd.map(lambda row: int(row[KEY_COLUMN]))

combined_keys_rdd = left_rdd_counts.union(right_rdd_counts)

#STEP 1: Compute key frequency histogram
key_freqs_rdd = combined_keys_rdd.map(lambda k: (k, 1)).reduceByKey(lambda a, b: a + b)
key_freqs_sorted = key_freqs_rdd.sortByKey()  

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

#Step 4

def get_partition_pid(key: int, partition_list: List[Tuple[int, int, int]]) -> int:
    for pid, start, end in partition_list:
        if start <= key <= end:
            return pid
    return -1

partitions = compute_key_ranges(key_freqs_sorted)

broadcast_partitions = spark.sparkContext.broadcast(partitions)

# left_df_rdd = spark.read.option("header", True).csv(HDFS_LEFT_INPUT_PATH).rdd

left_with_pid_rdd = left_rdd.map(lambda row: row.asDict()) \
    .map(lambda d: {**d, "pid": get_partition_pid(int(d[KEY_COLUMN]), broadcast_partitions.value)})
# === Step 6: Convert to DataFrame ===
left_with_pid_df = left_with_pid_rdd.map(lambda d: Row(**d)).toDF()


# right_df_rdd = spark.read.option("header", True).csv(HDFS_RIGHT_INPUT_PATH).rdd

right_with_pid_rdd = right_rdd.map(lambda row: row.asDict()) \
    .map(lambda d: {**d, "pid": get_partition_pid(int(d[KEY_COLUMN]), broadcast_partitions.value)})
# === Step 6: Convert to DataFrame ===
right_with_pid_df = right_with_pid_rdd.map(lambda d: Row(**d)).toDF()

print(f"*******************Partitions: {NUM_PARTITIONS}********************")

NUM_PARTITIONS = len(partitions)

print(f"*******************Partitions: {NUM_PARTITIONS}********************")
left_with_pid_df.repartition(NUM_PARTITIONS, col("pid"))
right_with_pid_df.repartition(NUM_PARTITIONS, col("pid"))

# === Step 7: Perform Join and Write Output ===

joined_df = left_with_pid_df.join(
    right_with_pid_df,
    on=["pid", "key"],
    how="inner"
)


joined_df.write \
    .partitionBy("pid") \
    .option("header", True) \
    .mode("overwrite") \
    .csv("hdfs://nn:9000/data/output_partitioned_join")

# === (Optional) Show Sample ===
