mapreduce/
├── scripts/
│   ├── step1_key_frequency.py       # Count key frequencies across both tables (per mapper)
│   ├── step2_merge_and_partition.py # Merge key counts and compute key ranges (single reducer)
│   ├── step3_repartition_data.py    # Filter & write data per partition (like shuffle stage)
│   ├── step4_join_partitions.py     # Join each partition (final reducer)
│   ├── config.py                    # Config
│   └── run_pipeline.sh              # Shell script to run all steps in sequence
│
├── tmp/
│   ├── global_stats/                # Output from key frequency merge
│   │   └── global_stats.csv
│   ├── partition_plan/              # Stores partition boundaries
│   │   └── partitions.csv
│   └── worker_stats/                # Optional stats from each worker
│       ├── worker_0.csv
│       ├── worker_1.csv
│       └── ...
│
├── output/
│   ├── repartitioned/               # Filtered data per key range
│   │   ├── left/
│   │   └── right/
│   └── joined/                      # Final join result per partition
│       └── partition_0/
│       └── ...
