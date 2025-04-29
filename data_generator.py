import numpy as np
import pandas as pd
import argparse
import os

def generate_skewed_keys(num_rows, num_keys, zipf_param=2.0, seed=None):
    np.random.seed(seed)
    keys = np.random.zipf(zipf_param, num_rows)
    keys = np.clip(keys, 1, num_keys)
    return keys

def generate_table(num_rows, num_keys, zipf_param=2.0, table_name="left", seed=None):
    keys = generate_skewed_keys(num_rows, num_keys, zipf_param=zipf_param, seed=seed)
    df = pd.DataFrame({
        f'{table_name}_row_id': np.arange(num_rows),
        'key': keys,
        f'{table_name}_payload': np.random.randint(1, 100000, size=num_rows)
    })
    return df

def save_in_parts(df, table_name, num_parts=3, output_dir="."):
    table_dir = os.path.join(output_dir, table_name)
    os.makedirs(table_dir, exist_ok=True)
    chunks = np.array_split(df, num_parts)
    for i, chunk in enumerate(chunks):
        path = os.path.join(table_dir, f"partition_{i}.csv")
        chunk.to_csv(path, index=False)
        print(f"Saved {path} with {len(chunk)} rows.")

def main(args):
    experiment_dir = os.path.join(args.output_dir, args.experiment_name)
    os.makedirs(experiment_dir, exist_ok=True)

    print(f"Generating LEFT table with {args.num_rows_left} rows, {args.num_keys} keys, skew={args.zipf_param_left}...")
    left_df = generate_table(args.num_rows_left, args.num_keys, zipf_param=args.zipf_param_left, table_name="left", seed=42)
    save_in_parts(left_df, "left", num_parts=args.num_parts, output_dir=experiment_dir)

    print(f"Generating RIGHT table with {args.num_rows_right} rows, {args.num_keys} keys, skew={args.zipf_param_right}...")
    right_df = generate_table(args.num_rows_right, args.num_keys, zipf_param=args.zipf_param_right, table_name="right", seed=99)
    save_in_parts(right_df, "right", num_parts=args.num_parts, output_dir=experiment_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate skewed join data and split into CSV parts.")
    parser.add_argument("--experiment_name", type=str, required=True, help="Experiment folder name")
    parser.add_argument("--num_keys", type=int, required=True, help="Number of unique keys (key domain)")
    parser.add_argument("--num_rows_left", type=int, required=True, help="Number of rows in LEFT table")
    parser.add_argument("--num_rows_right", type=int, required=True, help="Number of rows in RIGHT table")
    parser.add_argument("--zipf_param_left", type=float, default=2.0, help="Zipf skew parameter for LEFT table")
    parser.add_argument("--zipf_param_right", type=float, default=2.0, help="Zipf skew parameter for RIGHT table")
    parser.add_argument("--num_parts", type=int, default=3, help="Number of parts (simulate nodes)")
    parser.add_argument("--output_dir", type=str, default="./data", help="Top directory to save experiments")

    args = parser.parse_args()
    main(args)
