#!/bin/bash

set -e  # Exit on any error

# create a test DB using the eBird sample data
SAMPLE_DATA_PATH='ebd_sampling_data_mar_2025.txt'
# compress the sample data into a .gz file
echo "Compressing $SAMPLE_DATA_PATH to $SAMPLE_DATA_PATH.gz"
gzip -k "$SAMPLE_DATA_PATH"
# .tar that
echo "Creating tarball ${SAMPLE_DATA_PATH%.txt}.tar"
tar -cvf "${SAMPLE_DATA_PATH%.txt}.tar" "$SAMPLE_DATA_PATH.gz"

# now use our scripts to do the rest

echo "Running EBD parser script"
../../src/cloaca/swan_lake/scripts/parse_ebd.sh ebd_sampling_data_mar_2025 ./ ./ 1

# delete .tar & .gz
rm -f "$SAMPLE_DATA_PATH.gz" "${SAMPLE_DATA_PATH%.txt}.tar"

rm -f ebd_sampling_data_mar_2025_sorted.db

../../src/cloaca/swan_lake/scripts/sort_edb_in_place_sort_of.sh ebd_sampling_data_mar_2025.db test_input.db

# delete the unsorted db
rm -f ebd_sampling_data_mar_2025.db