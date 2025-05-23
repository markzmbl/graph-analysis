#!/bin/bash

# Exit on error
#set -e

# Activate the conda environment
#source ~/.bashrc
#conda activate py313

# Benchmark configuration
OMEGA=25
BUFFER=10
START_DATE="2021-10-01"
END_DATE="2021-10-01"
TASK_QUEUE=5000000
GC_MAX="32GB"
GC_COOLDOWN=1000000
PROGRESS=false

# Output file for benchmark results
RESULTS_FILE="benchmark_results.csv"
echo "workers,execution_time_sec" > "$RESULTS_FILE"

# Define worker values to test
WORKERS_LIST=(1 2 4 8 16 32 64 128 256 512 1024)

# Run benchmarks for each worker setting
for workers in "${WORKERS_LIST[@]}"
do
  echo "Running benchmark with workers=$workers..."

  start_time=$(date +%s.%N)

  PYTHON_GIL=0 .venv313t/bin/python3.13 -O meebits.py \
    --start_date "$START_DATE" \
    --end_date "$END_DATE" \
    --buffer "$BUFFER" \
    --omega "$OMEGA" \
    --workers "$workers" \
    --task_queue "$TASK_QUEUE" \
    --gc_max "$GC_MAX" \
    --gc_cooldown "$GC_COOLDOWN" \
    --progress "$PROGRESS"

  end_time=$(date +%s.%N)
  execution_time=$(echo "$end_time - $start_time" | bc)

  echo "$workers,$execution_time" >> "$RESULTS_FILE"
done

echo "Benchmark complete. Results written to $RESULTS_FILE"