# scripts/step2_merge_and_partition.py

import os
import pandas as pd
from scripts.config import (
    LOCAL_TMP_WORKER_STATS_DIR,
    LOCAL_TMP_GLOBAL_STATS_FILE,
    LOCAL_TMP_PARTITION_PLAN_FILE,
    NUM_PARTITIONS,
    KEY_COLUMN
)

def load_worker_counts(stats_dir: str) -> pd.DataFrame:
    """Load and concatenate all worker key count CSVs."""
    files = [os.path.join(stats_dir, f) for f in os.listdir(stats_dir) if f.endswith(".csv")]
    dfs = [pd.read_csv(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

def merge_key_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Merge key counts and compute global totals."""
    return df.groupby(KEY_COLUMN, as_index=False).agg({"count": "sum"}).sort_values(KEY_COLUMN)

def compute_key_ranges(global_df: pd.DataFrame) -> pd.DataFrame:
    """Split keys into NUM_PARTITIONS based on cumulative frequency."""
    total = global_df["count"].sum()
    ideal_per_partition = total // NUM_PARTITIONS

    partitions = []
    cur_sum, pid = 0, 0
    start_key = None

    for _, row in global_df.iterrows():
        key, freq = row[KEY_COLUMN], row["count"]
        if start_key is None:
            start_key = key
        cur_sum += freq

        if cur_sum >= ideal_per_partition and pid < NUM_PARTITIONS - 1:
            partitions.append((pid, start_key, key))
            pid += 1
            cur_sum = 0
            start_key = None

    if start_key is not None:
        partitions.append((pid, start_key, global_df[KEY_COLUMN].iloc[-1]))

    return pd.DataFrame(partitions, columns=["partition_id", "start_key", "end_key"])

def main():
    print("Step 2: Merging key frequencies and computing key ranges...")

    os.makedirs(os.path.dirname(LOCAL_TMP_GLOBAL_STATS_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(LOCAL_TMP_PARTITION_PLAN_FILE), exist_ok=True)

    df = load_worker_counts(LOCAL_TMP_WORKER_STATS_DIR)
    global_stats = merge_key_counts(df)
    global_stats.to_csv(LOCAL_TMP_GLOBAL_STATS_FILE, index=False)

    partitions = compute_key_ranges(global_stats)
    partitions.to_csv(LOCAL_TMP_PARTITION_PLAN_FILE, index=False)

    print(f"[✔] Wrote global key counts to {LOCAL_TMP_GLOBAL_STATS_FILE}")
    print(f"[✔] Wrote partition plan to {LOCAL_TMP_PARTITION_PLAN_FILE}")

if __name__ == "__main__":
    main()
