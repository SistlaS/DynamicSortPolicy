from pyspark.sql import SparkSession
from pyspark.sql.functions import count, spark_partition_id
import random
import matplotlib.pyplot as plt
import pandas as pd

# ----------------------------------------
# 1. Start Spark with AQE and skew handling enabled
# ----------------------------------------
spark = SparkSession.builder \
    .appName("AQEPartitionSkewTest") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.skewedPartitionThresholdInBytes", 1024 * 1024) \
    .getOrCreate()

# ----------------------------------------
# 2. Generate skewed dataset
# ----------------------------------------
N = 10000
keys = []

# Key 1: 1/3
keys += [1] * (N // 3)

# Keys 2–8: 1/3 (uniformly)
for k in range(2, 9):
    keys += [k] * ((N // 3) // 7)

# Keys 9–10: 1/3
keys += [9] * (N // 6)
keys += [10] * (N // 6)

# Pad any missing records
while len(keys) < N:
    keys.append(random.choice([1, 9, 10]))

random.shuffle(keys)
data = list(zip(keys, range(N)))
df = spark.createDataFrame(data, ["key", "value"])

# ----------------------------------------
# 3. Trigger a shuffle with groupBy
# ----------------------------------------
grouped_df = df.groupBy("key").agg(count("*").alias("cnt"))

# Add partition ID to inspect post-shuffle layout
final_df = grouped_df.withColumn("partition_id", spark_partition_id())

# ----------------------------------------
# 4. Analyze post-AQE partition distribution
# ----------------------------------------
partition_sizes = (
    final_df.groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

# Collect results to driver for plotting
pdf = partition_sizes.toPandas()

# Plot
plt.figure(figsize=(6, 6))
plt.bar(pdf['partition_id'].astype(str), pdf['count'])
plt.xlabel('Partition ID (Post-AQE)')
plt.ylabel('Number of Records')
plt.title('Final Partition Distribution After AQE')
plt.tight_layout()
plt.savefig("aqe_partition_distribution.png")
plt.close()

# ----------------------------------------
# 5. Done
# ----------------------------------------
spark.stop()
