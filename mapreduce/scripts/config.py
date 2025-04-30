# ========================
# Experiment Settings
# ========================

EXPERIMENT_NAME = "exp-5k-5k-skew-2.0-1.1-10keys"

# Number of partitions for key range join
NUM_PARTITIONS = 3

# Key skew threshold (optional use)
SKEW_THRESHOLD_RATIO = 1.5

# ========================
# HDFS Input Paths
# ========================

HDFS_LEFT_INPUT_PATH = f"hdfs://nn:9000/data/{EXPERIMENT_NAME}/left/*.csv"
HDFS_RIGHT_INPUT_PATH = f"hdfs://nn:9000/data/{EXPERIMENT_NAME}/right/*.csv"

# ========================
# Local Intermediate Paths (for planning)
# ========================

LOCAL_TMP_WORKER_STATS_DIR = "tmp/worker_stats"
LOCAL_TMP_GLOBAL_STATS_FILE = "tmp/global_stats/global_stats.csv"
LOCAL_TMP_PARTITION_PLAN_FILE = "tmp/partition_plan/partitions.csv"

# ========================
# Local Output Paths
# ========================

# After filtering data per key range
LOCAL_REPARTITIONED_LEFT = "output/repartitioned/left/"
LOCAL_REPARTITIONED_RIGHT = "output/repartitioned/right/"

# Final joined output per partition
LOCAL_JOINED_OUTPUT_BASE = "output/joined/"

# ========================
# Schema Parameters
# ========================

KEY_COLUMN = "key"
LEFT_ROW_ID = "left_row_id"
RIGHT_ROW_ID = "right_row_id"
LEFT_PAYLOAD = "left_payload"
RIGHT_PAYLOAD = "right_payload"
