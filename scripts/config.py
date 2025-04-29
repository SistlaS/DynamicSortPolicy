# ========================
# Experiment-level settings
# ========================

# The base name of your experiment
# EXPERIMENT_NAME = "exp-30rows-1000keys-zipf2"  # Change if you generate new experiments
EXPERIMENT_NAME = "exp-1m-2m-10kkeys-zipf1.5-3parts"

# Number of partitions you want after repartitioning
NUM_PARTITIONS = 6  # Example (change depending on your balancing goal)

# Skew threshold (optional, if you treat very heavy keys differently)
SKEW_THRESHOLD_RATIO = 1.5  # e.g., if a single key >1.5x ideal partition size

# ========================
# HDFS input paths
# ========================

# Where the original partitions are stored
HDFS_LEFT_INPUT_PATH = f"hdfs://nn:9000/data/{EXPERIMENT_NAME}/left/*.csv"
HDFS_RIGHT_INPUT_PATH = f"hdfs://nn:9000/data/{EXPERIMENT_NAME}/right/*.csv"

# ========================
# HDFS output paths
# ========================

# After repartitioning, where to save
HDFS_LEFT_REPARTITIONED_OUTPUT_PATH = f"hdfs://nn:9000/data/repartitioned/{EXPERIMENT_NAME}/left/"
HDFS_RIGHT_REPARTITIONED_OUTPUT_PATH = f"hdfs://nn:9000/data/repartitioned/{EXPERIMENT_NAME}/right/"

# ========================
# Local temporary paths
# ========================

# Local worker stats output (per worker)
LOCAL_TMP_WORKER_STATS_DIR = "/tmp/worker_stats"

# Local master aggregation output
LOCAL_TMP_GLOBAL_STATS_FILE = "/tmp/global_stats/global_stats.csv"

# Local partition boundary plan
LOCAL_TMP_PARTITION_PLAN_FILE = "/tmp/partition_plan/partitions.csv"

# ========================
# Spark Master URL
# ========================

SPARK_MASTER_URL = "spark://master:7077"

# ========================
# Other configs
# ========================

# Column names
KEY_COLUMN = "key"
COUNT_COLUMN = "count"
