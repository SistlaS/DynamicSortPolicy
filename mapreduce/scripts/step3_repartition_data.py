# step3_repartition_data.py
import os
import pandas as pd
from config import (
    HDFS_LEFT_INPUT_PATH,
    HDFS_RIGHT_INPUT_PATH,
    LOCAL_TMP_PARTITION_PLAN_FILE,
    LOCAL_REPARTITIONED_LEFT,
    LOCAL_REPARTITIONED_RIGHT,
    KEY_COLUMN
)

from utils.io import read_hdfs_csv_glob

def load_partition_plan():
    df = pd.read_csv(LOCAL_TMP_PARTITION_PLAN_FILE)
    return [(int(row['pid']), int(row['start']), int(row['end'])) for _, row in df.iterrows()]

def filter_and_write_partitions(input_df, table_prefix, output_dir, partitions):
    os.makedirs(output_dir, exist_ok=True)
    for pid, start, end in partitions:
        part_df = input_df[
            (input_df[KEY_COLUMN] >= start) & (input_df[KEY_COLUMN] <= end)
        ]
        out_path = os.path.join(output_dir, f"partition_{pid}.csv")
        part_df.to_csv(out_path, index=False)
        print(f"[✔] {table_prefix} partition {pid} → {out_path} ({len(part_df)} rows)")

def main():
    print("[1] Loading partition plan...")
    partitions = load_partition_plan()

    print("[2] Reading data from HDFS...")
    left_df = read_hdfs_csv_glob(HDFS_LEFT_INPUT_PATH)
    right_df = read_hdfs_csv_glob(HDFS_RIGHT_INPUT_PATH)

    print("[3] Repartitioning LEFT table...")
    filter_and_write_partitions(left_df, "LEFT", LOCAL_REPARTITIONED_LEFT, partitions)

    print("[4] Repartitioning RIGHT table...")
    filter_and_write_partitions(right_df, "RIGHT", LOCAL_REPARTITIONED_RIGHT, partitions)

if __name__ == "__main__":
    main()
