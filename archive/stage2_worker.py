from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import Partitioner
import pandas as pd
import uuid
import glob
import os


boundaries_broadcast = spark.sparkContext.broadcast(boundaries)
# =======================
# 5. PARTITIONING AND SORTING
# =======================

# Define custom RangePartitioner
class RangePartitioner(Partitioner):
    def __init__(self, boundaries):
        self.boundaries = sorted(boundaries)
        self.numPartitions = len(boundaries) + 1

    def getPartition(self, key):
        for idx, boundary in enumerate(self.boundaries):
            if key <= boundary:
                return idx
        return len(self.boundaries)

# Prepare RDDs
left_rdd = left_df.rdd.map(lambda row: (row["key"], row))
right_rdd = right_df.rdd.map(lambda row: (row["key"], row))

# Partition by custom partitioner
range_partitioner = RangePartitioner(boundaries_broadcast.value)
left_partitioned = left_rdd.partitionBy(range_partitioner.numPartitions, range_partitioner)
right_partitioned = right_rdd.partitionBy(range_partitioner.numPartitions, range_partitioner)

# Sort within each partition
def sort_partition(iterator):
    return iter(sorted(iterator, key=lambda x: x[0]))

left_sorted = left_partitioned.mapPartitions(sort_partition)
right_sorted = right_partitioned.mapPartitions(sort_partition)

print("Partitioning and sorting done!")

# =======================
# 6. MERGE JOIN
# =======================

# Merge-join sorted partitions
def merge_join_partition(left_iter, right_iter):
    left = list(left_iter)
    right = list(right_iter)

    result = []
    i = j = 0

    while i < len(left) and j < len(right):
        lkey, lval = left[i]
        rkey, rval = right[j]

        if lkey == rkey:
            result.append((lkey, (lval, rval)))
            i += 1
            j += 1
        elif lkey < rkey:
            i += 1
        else:
            j += 1

    return iter(result)

# Perform the merge-join
joined_rdd = left_sorted.zipPartitions(right_sorted, preservesPartitioning=True)(merge_join_partition)

print("Merge join completed!")

# =======================
# 7. USE JOINED OUTPUT
# =======================

# Example: Save output
joined_rdd.map(lambda x: str(x)).saveAsTextFile("hdfs:///path/to/output/joined_result")

print("Joined output written to HDFS!")

# =======================
# END OF SCRIPT
# =======================
