from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import Partitioner
import pandas as pd
import uuid
import glob
import os

def read_all_stats(folder_pattern):
    files = glob.glob(folder_pattern + "/*/*.csv")  # Because Spark writes inside a folder
    all_stats = None
    for file in files:
        df = pd.read_csv(file)
        if all_stats is None:
            all_stats = df
        else:
            all_stats = pd.concat([all_stats, df])
    return all_stats

# Read left and right stats
left_all_stats = read_all_stats(LOCAL_TMP_STATS_DIR)
right_all_stats = read_all_stats(LOCAL_TMP_STATS_DIR)

# Combine left and right keys (if needed for outer join)
global_stats = pd.concat([left_all_stats, right_all_stats])
global_stats = global_stats.groupby("key").sum().reset_index()

# Save global stats
global_stats.to_csv(GLOBAL_STATS_FILE, index=False)
print(f"Global stats written to {GLOBAL_STATS_FILE}")

# =======================
# 4. MASTER SIDE: COMPUTE BOUNDARIES
# =======================

# Read global stats
df_stats = pd.read_csv(GLOBAL_STATS_FILE)

# Sort keys
df_stats = df_stats.sort_values("key").reset_index(drop=True)

# Calculate ideal records per partition
total_records = df_stats["count"].sum()
ideal_rec_per_partition = total_records // NUM_PARTITIONS

boundaries = []
current_sum = 0
current_keys = []

for _, row in df_stats.iterrows():
    key = int(row["key"])
    count = int(row["count"])

    current_sum += count
    current_keys.append(key)

    if current_sum >= ideal_rec_per_partition:
        boundaries.append(current_keys[-1])  # end key of partition
        current_sum = 0
        current_keys = []

# Broadcast the boundaries
boundaries_broadcast = spark.sparkContext.broadcast(boundaries)
print(f"Boundaries for partitioning: {boundaries}")
