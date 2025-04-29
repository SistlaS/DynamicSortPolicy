from pyspark.sql import SparkSession
import time

start_time = time.time()
spark = SparkSession.builder.appName("Baseline_Basic_Join").getOrCreate()

# Read CSVs
left_df = spark.read.csv("hdfs://nn:9000/data/exp-30rows-1000keys-zipf2/left", header=True, inferSchema=True)
right_df = spark.read.csv("hdfs://nn:9000/data/exp-30rows-1000keys-zipf2/right", header=True, inferSchema=True)

left_df = left_df.selectExpr("key", "row_id as left_row_id", "left_payload")
right_df = right_df.selectExpr("key", "row_id as right_row_id", "right_payload")

# Perform normal join
joined = left_df.join(right_df, on="key", how="inner")

# Save output
joined.write.mode("overwrite").csv("hdfs://nn:9000/output/shuffle_join/")

end_time = time.time()
print(f"Total Runtime: {end_time - start_time:.2f} seconds")

