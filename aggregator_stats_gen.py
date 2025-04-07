from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import glob

spark = SparkSession.builder \
    .appName("Aggregate Key Frequency Distributions") \
    .getOrCreate()

stats_input_file = "distr_stats_worker_*.csv"

csv_files = glob.glob(stats_input_file)
if not csv_files:
    print("No matching distribution stats files found.")
    spark.stop()
    exit()

df_all = None
for file in csv_files:
    df = spark.read.csv(file, header=True, inferSchema=True)
    if df_all is None:
        df_all = df
    else:
        df_all = df_all.union(df)

global_stats = df_all.groupBy("key").agg(
    spark_sum("count").alias("global_count")
).orderBy("global_count", ascending=False)

output_path = f"distr_stats_global.csv"


global_stats.coalesce(1).write \
    .option("header", True) \
    .csv(output_path)

spark.stop()
