from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
from config import (
    SPARK_MASTER_URL,
    KEY_COLUMN,
)

# --- Parse arguments ---
parser = argparse.ArgumentParser(description="Worker merge-join partitioned left and right tables.")
parser.add_argument("--partition_id", type=int, required=True)
args = parser.parse_args()

partition_id = args.partition_id

# --- Setup Spark session ---
spark = (
    SparkSession.builder
    .appName(f"MergeJoin_Partition{partition_id}")
    .master(SPARK_MASTER_URL)
    .getOrCreate()
)

# --- Paths ---
LEFT_PARTITION_PATH = f"hdfs://nn:9000/data/repartitioned/exp-30rows-1000keys-zipf2/left/partition_{partition_id}/*.csv"
RIGHT_PARTITION_PATH = f"hdfs://nn:9000/data/repartitioned/exp-30rows-1000keys-zipf2/right/partition_{partition_id}/*.csv"
MERGE_OUTPUT_PATH = f"hdfs://nn:9000/data/merged/exp-30rows-1000keys-zipf2/partition_{partition_id}"

# --- Read both sides ---
left_df = (
    spark.read
    .option("header", True)
    .csv(LEFT_PARTITION_PATH)
)

right_df = (
    spark.read
    .option("header", True)
    .csv(RIGHT_PARTITION_PATH)
)

# --- Merge join on key ---
print(f"Performing local merge-join for partition {partition_id}...")

joined_df = left_df.join(
    right_df,
    on=KEY_COLUMN,
    how="inner"  # or "outer" if you want full outer join
)

# --- Save merged output ---
joined_df.write.option("header", True).csv(MERGE_OUTPUT_PATH)

print(f"Merge-join completed for partition {partition_id}. Output saved to {MERGE_OUTPUT_PATH}")

spark.stop()
