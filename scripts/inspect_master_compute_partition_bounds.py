from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from config import (
    LOCAL_TMP_GLOBAL_STATS_FILE,
    LOCAL_TMP_PARTITION_PLAN_FILE,
    NUM_PARTITIONS,
    KEY_COLUMN,
    COUNT_COLUMN,
)
import os
import pandas as pd

# Create Spark session
spark = (
    SparkSession.builder
    .appName("inspect_master_compute_partition_bounds")
    .master("spark://master:7077")
    .getOrCreate()
)

# Read global stats file
input_path = f"hdfs://nn:9000/{os.path.dirname(LOCAL_TMP_GLOBAL_STATS_FILE)}/global_stats.csv_tmp/part-*.csv"

print(f"Reading global stats from {input_path}...")

df = (
    spark.read
    .option("header", True)
    .csv(input_path)
)

# Cast columns properly
df = df.select(
    col(KEY_COLUMN).cast("int").alias("key"),
    col("global_count").cast("int").alias("global_count")
).orderBy("key")

# Calculate total number of records
total_records = df.agg(spark_sum("global_count")).collect()[0][0]
ideal_records_per_partition = total_records // NUM_PARTITIONS

print(f"Total records: {total_records}")
print(f"Ideal records per partition: {ideal_records_per_partition}")

# Partition boundary calculation
partitions = []
current_sum = 0
current_keys = []
partition_id = 0

df_collect = df.collect()

for row in df_collect:
    key = row["key"]
    count = row["global_count"]

    current_sum += count
    current_keys.append(key)

    if current_sum >= ideal_records_per_partition and partition_id < NUM_PARTITIONS - 1:
        start_key = current_keys[0]
        end_key = current_keys[-1]
        partitions.append((partition_id, start_key, end_key, current_sum))

        current_sum = 0
        current_keys = []
        partition_id += 1

# Handle remaining keys
if current_keys:
    start_key = current_keys[0]
    end_key = current_keys[-1]
    partitions.append((partition_id, start_key, end_key, current_sum))

# Save partition boundaries locally as CSV
df_partitions = pd.DataFrame(partitions, columns=["partition_id", "start_key", "end_key", "num_records"])
local_save_path = LOCAL_TMP_PARTITION_PLAN_FILE

os.makedirs(os.path.dirname(local_save_path), exist_ok=True)
df_partitions.to_csv(local_save_path, index=False)

print(f"Partition plan written to {local_save_path}")

spark.stop()
