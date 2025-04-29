import pandas as pd
import os
from config import (
    LOCAL_TMP_PARTITION_PLAN_FILE,
)

# Set number of workers (you could also read from a config or args if you want)
NUM_WORKERS = 3

# Paths
partition_plan_path = LOCAL_TMP_PARTITION_PLAN_FILE
assignment_save_path = "/tmp/partition_assignment/partition_assignment.csv"

# Make sure output directory exists
os.makedirs(os.path.dirname(assignment_save_path), exist_ok=True)

# Load partition plan
df = pd.read_csv(partition_plan_path)

# Sort partitions by num_records (optional: descending if you want smarter load balancing)
df = df.sort_values(by="num_records", ascending=False)

# Assign partitions to workers in round robin (or greedy way)
worker_loads = [0] * NUM_WORKERS
assignment = []

for _, row in df.iterrows():
    # Find the worker with the least current load
    worker_id = worker_loads.index(min(worker_loads))
    
    assignment.append((row["partition_id"], worker_id))
    worker_loads[worker_id] += row["num_records"]

# Save assignment
df_assignment = pd.DataFrame(assignment, columns=["partition_id", "worker_id"])
df_assignment.to_csv(assignment_save_path, index=False)

print(f"Partition assignment saved to {assignment_save_path}")
