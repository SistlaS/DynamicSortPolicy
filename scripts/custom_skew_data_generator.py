import numpy as np
import pandas as pd
import argparse
import os

def generate_custom_skew_keys(num_rows: int, num_keys: int, seed: int = 42) -> np.ndarray:
    np.random.seed(seed)
    key_probs = np.zeros(num_keys)

    # Key = 1 takes 1/3 of total
    key_probs[0] = 1 / 3

    if num_keys > 300:
        # Keys 2–300 take next 1/3
        key_probs[1:300] = (1 / 3) / 299

        # Keys 301+ take remaining 1/3
        key_probs[300:] = (1 / 3) / (num_keys - 300)
    else:
        key_probs[1:] = (2 / 3) / (num_keys - 1)

    keys = np.random.choice(np.arange(1, num_keys + 1), size=num_rows, p=key_probs)
    return keys

def generate_table(num_rows, num_keys, table_name, skew=False, seed=42):
    if skew:
        keys = generate_custom_skew_keys(num_rows, num_keys, seed=seed)
    else:
        np.random.seed(seed)
        keys = np.random.randint(1, num_keys + 1, size=num_rows)

    payload = np.random.randint(100000, size=num_rows)
    df = pd.DataFrame({
        f"{table_name}_row_id": np.arange(num_rows),
        "key": keys,
        f"{table_name}_payload": payload
    })
    return df

def save_in_parts(df, table_name, num_parts, output_dir):
    table_dir = os.path.join(output_dir, table_name)
    os.makedirs(table_dir, exist_ok=True)
    chunks = np.array_split(df, num_parts)
    for i, chunk in enumerate(chunks):
        path = os.path.join(table_dir, f"partition_{i}.csv")
        chunk.to_csv(path, index=False)
        print(f"✅ Saved {path} with {len(chunk)} rows.")

def main(args):
    experiment_dir = os.path.join(args.output_dir, args.experiment_name)
    os.makedirs(experiment_dir, exist_ok=True)

    print(f"🔹 Generating LEFT table (skewed={args.skew_left})...")
    left_df = generate_table(
        args.num_rows_left, args.num_keys, table_name="left", skew=args.skew_left, seed=42
    )
    save_in_parts(left_df, "left", args.num_parts, experiment_dir)

    print(f"🔹 Generating RIGHT table (skewed={args.skew_right})...")
    right_df = generate_table(
        args.num_rows_right, args.num_keys, table_name="right", skew=args.skew_right, seed=99
    )
    save_in_parts(right_df, "right", args.num_parts, experiment_dir)

    print(f"✅ Done. Output written to: {experiment_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate skewed join data with controlled distribution.")
    parser.add_argument("--experiment_name", type=str, required=True, help="Folder name under output_dir")
    parser.add_argument("--output_dir", type=str, default="/data", help="Top-level output directory")
    parser.add_argument("--num_keys", type=int, required=True, help="Total number of unique keys")
    parser.add_argument("--num_rows_left", type=int, required=True, help="Number of rows in LEFT table")
    parser.add_argument("--num_rows_right", type=int, required=True, help="Number of rows in RIGHT table")
    parser.add_argument("--num_parts", type=int, default=3, help="Number of CSV chunks to simulate partitions")
    parser.add_argument("--skew_left", action="store_true", help="Apply skewed distribution to LEFT table")
    parser.add_argument("--skew_right", action="store_true", help="Apply skewed distribution to RIGHT table")

    args = parser.parse_args()
    main(args)
