#!/bin/bash
set -e

INPUT_UNSORTED_EBD_PATH="$1"
OUTPUT_SORTED_EBD_PATH="$2"
# destructive arg
DESTRUCTIVE_ARG="$3"
if [ "$DESTRUCTIVE_ARG" == "true" ]; then
  echo "Running in destructive mode"
  # perform destructive actions
fi

# get size of DB on disk
unsorted_db_size=$(du -sh $INPUT_UNSORTED_EBD_PATH | cut -f1)
echo "Unsorted DB size: $unsorted_db_size"

# create unsorted table
echo "Creating unsorted table..."
duckdb $OUTPUT_SORTED_EBD_PATH <<EOF
ATTACH DATABASE '$INPUT_UNSORTED_EBD_PATH' AS ebd_unsorted;

CREATE TABLE ebd_sorted AS
FROM
  ebd_unsorted.ebd_full
WITH
  NO DATA;
EOF

input_row_count=$(duckdb $INPUT_UNSORTED_EBD_PATH <<EOF
SELECT COUNT(*) FROM ebd_full;
EOF
)

echo "Input row count: " 
echo "$input_row_count"

months_of_year=(1 2 3 4 5 6 7 8 9 10 11 12)

# loop over months
for MONTH in "${months_of_year[@]}"; do
    echo "[$(date +'%H:%M:%S')] Processing month: $MONTH"
    START_TIME=$(date +%s)
    duckdb $OUTPUT_SORTED_EBD_PATH <<EOF
    ATTACH DATABASE '$INPUT_UNSORTED_EBD_PATH' AS ebd_unsorted;

    INSERT INTO
    ebd_sorted
    FROM
    ebd_unsorted.ebd_full
    where
    extract (
        month
        from
        observation_date
    ) = $MONTH
    ORDER BY
    observation_date;
EOF
    END_TIME=$(date +%s)
    ELAPSED_TIME=$((END_TIME - START_TIME))
    ELAPSED_MINUTES=$((ELAPSED_TIME / 60))
    ELAPSED_SECONDS=$((ELAPSED_TIME % 60))
    echo "[$(date +'%H:%M:%S')] Processing month: $MONTH completed"
    if [ ${ELAPSED_MINUTES} -gt 0 ]; then
        echo "Total execution time: ${ELAPSED_MINUTES}m ${ELAPSED_SECONDS}s"
    else
        echo "Total execution time: ${ELAPSED_SECONDS}s"
    fi
done


output_row_count=$(duckdb $OUTPUT_SORTED_EBD_PATH <<EOF
SELECT COUNT(*) FROM ebd_sorted;
EOF
)

echo "Input row count: " 
echo "$input_row_count"
echo "Output row count: " 
echo "$output_row_count"

echo "Unsorted DB size: $unsorted_db_size"
sorted_db_size=$(du -sh $OUTPUT_SORTED_EBD_PATH | cut -f1)
echo "Sorted DB size: $sorted_db_size"
