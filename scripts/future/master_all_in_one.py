from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import pandas as pd
import os

# ===================
# Configuration
# ===================
from config import (
    LOCAL_TMP_WORKER_STATS_DIR,
    LOCAL_TMP_GLOBAL_STATS_FILE,
    LOCAL_TMP_PARTITION_PLAN_FILE,
    NUM_PARTITIONS,
    KEY_COLUMN,
    COUNT_COLUMN,
    SPARK_MASTER_URL,
)

# Number of workers in the cluster
NUM_WORKERS = 3

# ===================
# Full Master Pipeline
# ===================

spark = (
    SparkSession.builder
    .appName("Master Aggregate, Partition, Assign")
    .master(SPARK_MASTER_URL)
    .getOrCreate()
)

# Step 1: Aggregate key frequencies from all workers
def aggregate_worker_stats():
    input_path = f"hdfs://nn:9000/{LOCAL_TMP_WORKER_STATS_DIR}/worker_*/part-*.csv"
    print(f"Reading worker stats from {input_path}...")

    df = (
        spark.read
        .option("header", True)
        .csv(input_path)
    )

    df = df.select(
        col(KEY_COLUMN).cast("int").alias("key"),
        col(COUNT_COLUMN).cast("int").alias("count")
    )

    df_global = (
        df.groupBy("key")
        .agg(spark_sum("count").alias("global_count"))
        .orderBy("key")
    )

    output_tmp_path = f"hdfs://nn:9000/{LOCAL_TMP_GLOBAL_STATS_FILE}_tmp"
    df_global.coalesce(1).write.option("header", True).csv(output_tmp_path)

    print(f"Global key stats saved to {output_tmp_path}")

# Step 2 and 3: Compute partitions and assign workers
def compute_partitions_and_assign():
    input_path = f"hdfs://nn:9000/{os.path.dirname(LOCAL_TMP_GLOBAL_STATS_FILE)}/global_stats.csv_tmp/part-*.csv"
    print(f"Reading global stats from {input_path}...")

    df = (
        spark.read
        .option("header", True)
        .csv(input_path)
    )

    df = df.select(
        col(KEY_COLUMN).cast("int").alias("key"),
        col("global_count").cast("int").alias("global_count")
    ).orderBy("key")

    total_records = df.agg(spark_sum("global_count")).collect()[0][0]
    ideal_records_per_partition = total_records // NUM_PARTITIONS

    print(f"Total records: {total_records}")
    print(f"Ideal records per partition: {ideal_records_per_partition}")

    partitions = []
    current_sum = 0
    current_records = 0
    current_keys = []
    partition_id = 0

    df_collect = df.collect()

    for row in df_collect:
        key = row["key"]
        count = row["global_count"]

        current_sum += count
        current_records += count
        current_keys.append(key)

        if current_sum >= ideal_records_per_partition and partition_id < NUM_PARTITIONS - 1:
            start_key = current_keys[0]
            end_key = current_keys[-1]
            worker_id = partition_id % NUM_WORKERS

            partitions.append((partition_id, start_key, end_key, current_records, worker_id))

            current_sum = 0
            current_records = 0
            current_keys = []
            partition_id += 1

    if current_keys:
        start_key = current_keys[0]
        end_key = current_keys[-1]
        worker_id = partition_id % NUM_WORKERS
        partitions.append((partition_id, start_key, end_key, current_records, worker_id))

    df_partitions = pd.DataFrame(partitions, columns=["partition_id", "start_key", "end_key", "num_records", "worker_id"])

    local_save_path = LOCAL_TMP_PARTITION_PLAN_FILE
    os.makedirs(os.path.dirname(local_save_path), exist_ok=True)
    df_partitions.to_csv(local_save_path, index=False)

    print(f"Partition+Assignment plan written to {local_save_path}")

# Run pipeline
aggregate_worker_stats()
compute_partitions_and_assign()

spark.stop()
