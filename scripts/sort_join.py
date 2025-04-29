from pyspark.sql import SparkSession

import time
start_time = time.time()

spark = SparkSession.builder \
    .appName("Baseline_SortMerge") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

left_df = spark.read.csv("hdfs://nn:9000/data/exp-30rows-1000keys-zipf2/left", header=True, inferSchema=True)
right_df = spark.read.csv("hdfs://nn:9000/data/exp-30rows-1000keys-zipf2/right", header=True, inferSchema=True)


# Perform sort-merge join
joined = left_df.join(right_df, on="key", how="inner")

# Save output
joined.write.mode("overwrite").csv("hdfs://nn:9000/output/sortmerge/")

end_time = time.time()
print(f"Total Runtime: {end_time - start_time:.2f} seconds")
