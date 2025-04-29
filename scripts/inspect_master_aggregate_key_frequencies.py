from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from config import (
    LOCAL_TMP_WORKER_STATS_DIR,
    LOCAL_TMP_GLOBAL_STATS_FILE,
    KEY_COLUMN
)

# Create Spark session
spark = (
    SparkSession.builder
    .appName("inspect_master_aggregate_key_frequencies")
    .master("spark://master:7077")
    .getOrCreate()
)

# Read all worker output files
input_path = f"hdfs://nn:9000/{LOCAL_TMP_WORKER_STATS_DIR}/worker_*/part-*.csv"

print(f"Reading worker stats from {input_path}...")

df = (
    spark.read
    .option("header", True)
    .csv(input_path)
)

# Cast key and count columns properly
df = df.select(
    col(KEY_COLUMN).cast("int").alias("key"),
    col("count").cast("int").alias("count")
)

# Aggregate counts across workers
df_global = (
    df.groupBy("key")
    .agg(spark_sum("count").alias("global_count"))
    .orderBy("key")
)

# Save to output
output_tmp_path = f"hdfs://nn:9000/{LOCAL_TMP_GLOBAL_STATS_FILE}_tmp"

df_global.coalesce(1).write.option("header", True).csv(output_tmp_path)

print(f"Global key stats saved to {output_tmp_path}")

spark.stop()
