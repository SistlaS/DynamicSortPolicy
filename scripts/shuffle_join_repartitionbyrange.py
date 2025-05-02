from pyspark.sql import SparkSession
import time
from config import (
    EXPERIMENT_NAME,
    KEY_COLUMN,
    NUM_PARTITIONS,
    SPARK_MASTER_URL,
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
)

start_time = time.time()

spark = SparkSession.builder \
    .appName(f"PartitionByRange_SortMerge-{EXPERIMENT_NAME}") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .config("spark.executor.memory", "25g") \
    .config("spark.driver.memory", "25g") \
    .config("spark.sql.shuffle.partitions", str(NUM_PARTITIONS)) \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs://nn:9000/spark-logs/") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://nn:9000") \
    .getOrCreate()

# Load CSVs
left_df = spark.read.csv(HDFS_LEFT_INPUT_PATH, header=True, inferSchema=True)
right_df = spark.read.csv(HDFS_RIGHT_INPUT_PATH, header=True, inferSchema=True)

# Repartition using range partitioning to reduce skew
left_df_repart = left_df.repartitionByRange(NUM_PARTITIONS, KEY_COLUMN)
right_df_repart = right_df.repartitionByRange(NUM_PARTITIONS, KEY_COLUMN)

# Sort-merge join on the range-partitioned data
joined = left_df_repart.join(right_df_repart, on=KEY_COLUMN, how="inner")

# Save output
joined.write.mode("overwrite").csv("hdfs://nn:9000/output/sortmerge_partitioned/")

spark.stop()
end_time = time.time()
print(f"Total Runtime: {end_time - start_time:.2f} seconds")
