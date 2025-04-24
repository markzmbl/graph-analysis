#!/bin/bash

set -e

# Set environment variables
export PYPY_GC_MAX=16GB

# Define paths to virtual environments
VENV_THREAD=".venv313t/bin/python"
VENV_PROCESS=".venv313/bin/python"
VENV_PYPY=".venv311p/bin/python"

SCRIPT="meebits.py"
#WORKERS_LIST=(1 2 4 8)
WORKERS_LIST=(2 4 8)

# Benchmark runner functions
run_benchmark_pypy() {
    local workers=$1
    export PYTHON_GIL=1
    echo "Running $SCRIPT with PyPy and $workers workers..."
    { time "$VENV_PYPY" -O "$SCRIPT" --workers "$workers" --_threaded 0; } 2>&1 | tee -a benchmark_results.txt
    echo "-------------------------"
}

run_benchmark_cpython() {
    local workers=$1
    export PYTHON_GIL=1
    echo "Running $SCRIPT with CPython and $workers workers..."
    { time "$VENV_PROCESS" -O "$SCRIPT" --workers "$workers" --_threaded 0; } 2>&1 | tee -a benchmark_results.txt
    echo "-------------------------"
}

run_benchmark_cpython_threaded() {
    local workers=$1
    export PYTHON_GIL=0
    echo "Running $SCRIPT with free-threaded CPython and $workers workers..."
    { time "$VENV_THREAD" -O "$SCRIPT" --workers "$workers" --_threaded 1; } 2>&1 | tee -a benchmark_results.txt
    echo "-------------------------"
}

# Initialize results
echo "Benchmarking Results - $(date)" > benchmark_results.txt
echo "=== Benchmarking with 0 workers ===" | tee -a benchmark_results.txt

#run_benchmark_pypy 0
#run_benchmark_cpython 0

for workers in "${WORKERS_LIST[@]}"; do
    echo "=== Benchmarking with $workers workers ===" | tee -a benchmark_results.txt

    run_benchmark_pypy "$workers"
    run_benchmark_cpython_threaded "$workers"
#    run_benchmark_cpython "$workers"

    echo "=========================================" | tee -a benchmark_results.txt
done

echo "Benchmarking complete! Results saved in benchmark_results.txt"