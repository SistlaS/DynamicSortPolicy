import os
import pandas as pd
from config import (
    NUM_PARTITIONS,
    LOCAL_REPARTITIONED_LEFT,
    LOCAL_REPARTITIONED_RIGHT,
    LOCAL_JOINED_OUTPUT_BASE,
    KEY_COLUMN
)

def join_partitions():
    os.makedirs(LOCAL_JOINED_OUTPUT_BASE, exist_ok=True)

    for pid in range(NUM_PARTITIONS):
        left_path = os.path.join(LOCAL_REPARTITIONED_LEFT, f"partition_{pid}.csv")
        right_path = os.path.join(LOCAL_REPARTITIONED_RIGHT, f"partition_{pid}.csv")
        output_path = os.path.join(LOCAL_JOINED_OUTPUT_BASE, f"partition_{pid}")

        print(f"[Joining] Partition {pid}: {left_path} ⨝ {right_path}")
        left_df = pd.read_csv(left_path)
        right_df = pd.read_csv(right_path)

        joined_df = pd.merge(left_df, right_df, on=KEY_COLUMN, how="inner")

        os.makedirs(output_path, exist_ok=True)
        joined_df.to_csv(os.path.join(output_path, "joined.csv"), index=False)
        print(f"[✓] Written joined output to: {output_path}/joined.csv")

if __name__ == "__main__":
    join_partitions()
