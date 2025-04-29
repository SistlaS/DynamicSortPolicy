from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
from config import (
    KEY_COLUMN,
    SPARK_MASTER_URL,
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
    HDFS_LEFT_REPARTITIONED_OUTPUT_PATH,
    HDFS_RIGHT_REPARTITIONED_OUTPUT_PATH,
)

# --- Parse input arguments ---
parser = argparse.ArgumentParser(description="Worker shuffle both left and right tables.")
parser.add_argument("--start_key", type=int, required=True)
parser.add_argument("--end_key", type=int, required=True)
parser.add_argument("--partition_id", type=int, required=True)
args = parser.parse_args()

# --- Setup Spark ---
spark = (
    SparkSession.builder
    .appName(f"ShuffleWorker_Partition{args.partition_id}")
    .master(SPARK_MASTER_URL)
    .getOrCreate()
)

# --- Filter LEFT table ---
print(f"Filtering LEFT side for keys {args.start_key} to {args.end_key}...")

left_df = (
    spark.read.option("header", True)
    .csv(HDFS_LEFT_INPUT_PATH)
    .filter((col(KEY_COLUMN) >= args.start_key) & (col(KEY_COLUMN) <= args.end_key))
)

left_output_path = f"{HDFS_LEFT_REPARTITIONED_OUTPUT_PATH}/partition_{args.partition_id}"
left_df.write.option("header", True).csv(left_output_path)

# --- Filter RIGHT table ---
print(f"Filtering RIGHT side for keys {args.start_key} to {args.end_key}...")

right_df = (
    spark.read.option("header", True)
    .csv(HDFS_RIGHT_INPUT_PATH)
    .filter((col(KEY_COLUMN) >= args.start_key) & (col(KEY_COLUMN) <= args.end_key))
)

right_output_path = f"{HDFS_RIGHT_REPARTITIONED_OUTPUT_PATH}/partition_{args.partition_id}"
right_df.write.option("header", True).csv(right_output_path)

print(f"Partition {args.partition_id} shuffle completed for both sides!")

spark.stop()
