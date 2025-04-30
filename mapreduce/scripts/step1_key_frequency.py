# scripts/step1_key_frequency.py

import pandas as pd
import os
import sys
from collections import Counter
from config import (
    KEY_COLUMN,
    LOCAL_TMP_WORKER_STATS_DIR
)

def count_keys(input_path: str, worker_id: int):
    print(f"[Worker {worker_id}] Reading: {input_path}")
    df = pd.read_csv(input_path)
    keys = df[KEY_COLUMN].astype(int)
    counter = Counter(keys)

    # Write key frequency to tmp/worker_stats/worker_<id>.csv
    os.makedirs(LOCAL_TMP_WORKER_STATS_DIR, exist_ok=True)
    output_path = os.path.join(LOCAL_TMP_WORKER_STATS_DIR, f"worker_{worker_id}.csv")
    pd.DataFrame(list(counter.items()), columns=[KEY_COLUMN, "count"]).to_csv(output_path, index=False)
    print(f"[Worker {worker_id}] Wrote key counts to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python step1_key_frequency.py <input_csv_path> <worker_id>")
        sys.exit(1)

    input_csv_path = sys.argv[1]
    worker_id = int(sys.argv[2])
    count_keys(input_csv_path, worker_id)
