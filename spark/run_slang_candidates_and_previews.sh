#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG="${CONFIG:-spark/fabric/config.json}"
LOG_DIR="${LOG_DIR:-spark/logs}"
PYTHON_BIN="${PYTHON_BIN:-/home/ubuntu/miniconda3/envs/spark22/bin/python}"

mkdir -p "$LOG_DIR"

COMMON_SPARK_ARGS=(
  --master yarn
  --deploy-mode client
  --num-executors 3
  --executor-cores 2
  --executor-memory 4g
  --driver-memory 2g
  --conf spark.yarn.executor.memoryOverhead=1024
  --conf spark.yarn.am.memory=2g
  --conf spark.yarn.am.memoryOverhead=1024
  --conf spark.sql.shuffle.partitions=600
  --conf spark.pyspark.python="$PYTHON_BIN"
  --conf spark.pyspark.driver.python="$PYTHON_BIN"
  --conf spark.executorEnv.PYSPARK_PYTHON="$PYTHON_BIN"
  --conf spark.local.dir=/mnt/spark-local
)

run_spark_job() {
  local log_path="$1"
  shift

  echo "Running: $*"
  echo "Logging to: $log_path"

  spark-submit "${COMMON_SPARK_ARGS[@]}" "$@" 2>&1 | tee "$log_path"
}

# -------------------------
# Build slang candidate tables
# -------------------------
run_spark_job \
  "$LOG_DIR/slang_candidates_clean_ratio_run.log" \
  spark/fabric/slang_candidates.py \
  --config "$CONFIG" \
  --mode subreddit

for time_grain in monthly yearly; do
  run_spark_job \
    "$LOG_DIR/slang_candidates_spiking_${time_grain}_run.log" \
    spark/fabric/slang_candidates.py \
    --config "$CONFIG" \
    --mode subreddit \
    --spike-time-grain "$time_grain"
done

# -------------------------
# Preview slang candidate tables
# -------------------------
for sort_name in ratio count; do
  run_spark_job \
    "$LOG_DIR/preview_slang_candidates_clean_ratio_monthly_sort_${sort_name}.log" \
    spark/fabric/preview/preview_slang_candidates.py \
    --config "$CONFIG" \
    --mode subreddit \
    --candidate-type clean_ratio \
    --spike-time-grain monthly \
    --sort "$sort_name" \
    --limit 2000
done

for time_grain in monthly yearly; do
  for sort_name in ratio spike count; do
    run_spark_job \
      "$LOG_DIR/preview_slang_candidates_spiking_${time_grain}_sort_${sort_name}.log" \
      spark/fabric/preview/preview_slang_candidates.py \
      --config "$CONFIG" \
      --mode subreddit \
      --candidate-type spiking \
      --spike-time-grain "$time_grain" \
      --sort "$sort_name" \
      --limit 2000
  done
done
