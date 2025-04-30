#!/bin/bash
set -e

echo "[STEP 1] Counting key frequencies..."
python3 scripts/step1_key_frequency.py

echo "[STEP 2] Merging stats and generating key ranges..."
python3 scripts/step2_merge_and_partition.py

echo "[STEP 3] Repartitioning data by key ranges..."
python3 scripts/step3_repartition_data.py

echo "[STEP 4] Joining each key range partition..."
python3 scripts/step4_join_partitions.py

echo "[✔] Pipeline complete. Joined output is in: output/joined/"
