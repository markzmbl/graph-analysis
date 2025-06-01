#!/bin/bash

# Ensure the script exits on first error
#set -e

# Activate the conda environment
source ~/.bashrc
conda activate py313

# Benchmark configuration
OMEGA_VALUES=(10 20 25 30 40 50 60 70 80 90 100 150 200 300 250 400 500 350 450 1000)
BUFFER=10
START_DATE="2021-10-01"
END_DATE="2022-07-01"
GC_MAX="200GB"
LOG_INTERVAL=60
LOG_PREFIX="may-run2"
LOG_DIR="/export/share/markusz33dm/logs/"
PROGRESS=true

# Output file for benchmark results
RESULTS_FILE="$LOG_DIR/benchmark_results.csv"
echo "omega,execution_time_sec" > "$RESULTS_FILE"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Run the benchmarks
for omega in "${OMEGA_VALUES[@]}"
do
  # Pad omega to 4 digits (e.g., 0025)
  omega_padded=$(printf "%04d" "$omega")

  echo "Running benchmark for omega=$omega..."

  start_time=$(date +%s.%N)

  PYTHON_GIL=0 python3.13t -O meebits.py \
    --start_date "$START_DATE" \
    --end_date "$END_DATE" \
    --buffer "$BUFFER" \
    --omega "$omega" \
    --gc_max "$GC_MAX" \
    --log_interval "$LOG_INTERVAL" \
    --log_prefix "$LOG_PREFIX-omega$omega_padded" \
    --log_dir "$LOG_DIR" \
    --progress "$PROGRESS"

  end_time=$(date +%s.%N)
  execution_time=$(echo "$end_time - $start_time" | bc)

  echo "$omega,$execution_time" >> "$RESULTS_FILE"
done

echo "Benchmark complete. Results written to $RESULTS_FILE"
