#!/bin/bash

# Start date
START_DATE="2020-01-01"

# Omega value
OMEGA=10

# Cycle length
CYCLE_LENGTH=80

# Cleanup interval
CLEANUP_INTERVAL=100000

# Base directories
OUTPUT_DIR="/export/share/markusz33dm/result"
TMP_DIR="/export/share/markusz33dm/tmp"

# Ensure the output and temp directories exist
mkdir -p "$OUTPUT_DIR" "$TMP_DIR"

# List of durations to loop over
DURATIONS=("1 day" "2 days" "3 days" "4 days" "7 days" "14 days" "1 month" "3 months" "6 months" "9 months" "12 months")

# Loop over each duration
for DURATION in "${DURATIONS[@]}"; do
    # Calculate end date
    END_DATE=$(date -d "$START_DATE + $DURATION" +%Y-%m-%d)

    # Format the file prefix
    FILE_PREFIX="${OUTPUT_DIR}/2scent_$(echo "$DURATION" | tr ' ' '_')"

    # Run the Python script
    python3 run_2scent.py \
        --start-date "$START_DATE" \
        --end-date "$END_DATE" \
        --omega "$OMEGA" \
        --cycle-length "$CYCLE_LENGTH" \
        --cleanup-interval "$CLEANUP_INTERVAL" \
        --file-prefix "$FILE_PREFIX" \
        --temporary-directory "$TMP_DIR"

    echo "Processed: $START_DATE to $END_DATE (Duration: $DURATION), output in $FILE_PREFIX, temp files in $TMP_DIR"
done