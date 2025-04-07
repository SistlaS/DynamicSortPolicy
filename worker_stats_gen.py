from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Key Frequency Distribution") \
    .getOrCreate()

csv_path = "/data/left_node_1.csv"

df = spark.read.csv(csv_path, header=True, inferSchema=True)

key_freq = df.groupBy("key").count().orderBy("count", ascending=False)

worker_id = spark.sparkContext.applicationId.replace("-", "_")
output_path = f"distr_stats_worker_{worker_id}.csv"

key_freq.coalesce(1).write \
    .option("header", True) \
    .csv(output_tmp_path)

spark.stop()
