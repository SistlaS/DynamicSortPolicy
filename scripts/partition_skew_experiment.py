from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id
import random
import matplotlib.pyplot as plt
from collections import Counter
import pandas as pd

# ----------------------------------------
# 1. Start Spark
# ----------------------------------------
spark = SparkSession.builder \
    .appName("SkewedPartitionTest") \
    .getOrCreate()

# ----------------------------------------
# 2. Generate skewed dataset (100 keys)
# - Key 1: 1/3
# - Keys 2–99: 1/3 total (~34 each)
# - Key 100: 1/3
# ----------------------------------------
N = 10000
keys = []

# Key 1 → ~3333
keys += [1] * (N // 3)

# Keys 2–99 → ~3333 across 98 keys ≈ 34 each
for k in range(2, 100):
    keys += [k] * ((N // 3) // 98)

# Key 100 → ~3333
keys += [100] * (N // 3)

# Fill any remaining due to rounding
while len(keys) < N:
    keys.append(random.choice([1, 100]))

# Shuffle and assign values
random.shuffle(keys)
data = list(zip(keys, range(N)))
df = spark.createDataFrame(data, ["key", "value"])

# ----------------------------------------
# 3. Plot key frequency distribution
# ----------------------------------------
key_counts = Counter(keys)
key_df = pd.DataFrame(list(key_counts.items()), columns=["key", "count"]).sort_values("key")

plt.figure(figsize=(6, 6))
plt.bar(key_df["key"], key_df["count"], width=0.8)  # narrower bars

plt.xlabel('Key')
plt.ylabel('Frequency')
plt.title('Skewness of Join Key (Keys 1 and 100 Dominant)')
plt.xticks(
    ticks=[k for k in range(0, 101, 10)],
    labels=[str(k) for k in range(0, 101, 10)]
)
plt.grid(axis='y', linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.savefig("key_skew_plot.png")
plt.close()



# ----------------------------------------
# 4. Repartition using hash partitioning
# ----------------------------------------
num_partitions = 10
df_partitioned = df.repartition(num_partitions, col("key"))

# ----------------------------------------
# 5. Analyze and plot partition distribution
# ----------------------------------------
partition_sizes = (
    df_partitioned
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

pdf = partition_sizes.toPandas().sort_values("partition_id")
partition_ids = pdf['partition_id'].astype(str)
counts = pdf['count']

plt.figure(figsize=(6, 6))
plt.bar(partition_ids, counts)
plt.xlabel('Partition ID')
plt.ylabel('Number of Records')
plt.title('Record Distribution Across Partitions (Hash Partitioning)')
plt.tight_layout()
plt.savefig("partition_distribution_plot_fixed.png")
plt.close()

# ----------------------------------------
# 6. Stop Spark
# ----------------------------------------
spark.stop()
