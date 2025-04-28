from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import Partitioner
import pandas as pd
import uuid
import glob
import os

# =======================
# 1. SETUP
# =======================
spark = SparkSession.builder.appName("DistributedMergeJoin").getOrCreate()

# Paths
#TODO: How do we allocate this paths to each worker initially?
LEFT_INPUT = "hdfs:///path/to/left.csv"
RIGHT_INPUT = "hdfs:///path/to/right.csv"
LOCAL_TMP_STATS_DIR = "/tmp/stats/"
GLOBAL_STATS_FILE = "/tmp/global_stats.csv"
NUM_PARTITIONS = 6

# Clean temp dir if exists
if not os.path.exists(LOCAL_TMP_STATS_DIR):
    os.makedirs(LOCAL_TMP_STATS_DIR)

# =======================
# 2. WORKER SIDE: GENERATE STATS
# =======================

# Read input datasets
left_df = spark.read.csv(LEFT_INPUT, header=True, inferSchema=True)
right_df = spark.read.csv(RIGHT_INPUT, header=True, inferSchema=True)

# Group by key and count
left_key_counts = left_df.groupBy("key").count()
right_key_counts = right_df.groupBy("key").count()

# Save local stats
left_tmp_stats_path = LOCAL_TMP_STATS_DIR + f"left_stats_{uuid.uuid4()}.csv"
right_tmp_stats_path = LOCAL_TMP_STATS_DIR + f"right_stats_{uuid.uuid4()}.csv"

left_key_counts.coalesce(1).write.option("header", True).csv(left_tmp_stats_path)
right_key_counts.coalesce(1).write.option("header", True).csv(right_tmp_stats_path)

# Wait until write is done
spark.sparkContext.setJobGroup("Stats Write", "Writing local stats to tmp dir")

spark.sparkContext.cancelAllJobs()  # Safe in script - ensure stats write completes before master proceeds

print("Local stats generation done!")
