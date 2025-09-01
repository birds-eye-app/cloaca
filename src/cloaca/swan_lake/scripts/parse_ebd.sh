#!/bin/bash
set -e

# Usage function
usage() {
    echo "Usage: $0 <EBD_NAME> <DIR_WITH_TAR> <OUTPUT_DB_PATH> [OVERRIDE_TABLE]"
    echo ""
    echo "Arguments:"
    echo "  EBD_NAME         Name of the eBird dataset (e.g., ebd_US-NY-061_relJul-2025)"
    echo "  DIR_WITH_TAR     Directory containing the .tar file"
    echo "  OUTPUT_DB_PATH   Directory where the database will be created"
    echo "  OVERRIDE_TABLE   Optional: 1 to override existing table, 0 to append (default: 1)"
    echo ""
    echo "Example:"
    echo "  $0 ebd_US-NY-061_relJul-2025 /Users/user/Downloads/ /path/to/dbs/"
    exit 1
}

# Check arguments
if [ $# -lt 3 ]; then
    echo "Error: Missing required arguments"
    usage
fi

EBD_NAME="$1"
DIR_WITH_TAR="$2"
OUTPUT_DB_PATH="$3"
OVERRIDE_TABLE="${4:-1}"
OUTPUT_DB_NAME="${EBD_NAME}.db"

# Validate inputs
if [ ! -d "$DIR_WITH_TAR" ]; then
    echo "Error: Directory $DIR_WITH_TAR does not exist"
    exit 1
fi

if [ ! -f "${DIR_WITH_TAR}${EBD_NAME}.tar" ]; then
    echo "Error: File ${DIR_WITH_TAR}${EBD_NAME}.tar does not exist"
    exit 1
fi

if [ ! -d "$OUTPUT_DB_PATH" ]; then
    echo "Creating output directory: $OUTPUT_DB_PATH"
    mkdir -p "$OUTPUT_DB_PATH"
fi

# Start timing
START_TIME=$(date +%s)
echo "Input EBD .tar file: ${DIR_WITH_TAR}${EBD_NAME}.tar"
echo "Output database: ${OUTPUT_DB_PATH}${OUTPUT_DB_NAME}"

if [ ${OVERRIDE_TABLE} -eq 1 ]; then
    echo "[$(date +'%H:%M:%S')] Removing existing database file..."
    rm -f ${OUTPUT_DB_PATH}${OUTPUT_DB_NAME}
    echo "[$(date +'%H:%M:%S')] Database file removed"
else
    echo "[$(date +'%H:%M:%S')] Appending to existing database"
fi

QUERY="
SET preserve_insertion_order=false;

create or replace TABLE ebd_full AS
SELECT
    \"GLOBAL UNIQUE IDENTIFIER\" as global_unique_identifier,
    \"CATEGORY\" as category,
    \"COMMON NAME\" as common_name,
    \"SCIENTIFIC NAME\" as scientific_name,
    \"SUBSPECIES COMMON NAME\" as subspecies_common_name,
    \"SUBSPECIES SCIENTIFIC NAME\" as subspecies_scientific_name,
    \"EXOTIC CODE\" as exotic_code,
    \"OBSERVATION COUNT\" as observation_count,
    \"COUNTRY CODE\" as country_code,
    \"STATE CODE\" as state_code,
    \"COUNTY CODE\" as county_code,
    \"LOCALITY\" as locality,
    \"LOCALITY ID\" as locality_id,
    \"LOCALITY TYPE\" as locality_type,
    \"LATITUDE\" as latitude,
    \"LONGITUDE\" as longitude,
    \"OBSERVATION DATE\" as observation_date,
    \"SAMPLING EVENT IDENTIFIER\" as sampling_event_identifier,
    \"PROTOCOL NAME\" as protocol_name,
    \"DURATION MINUTES\" as duration_minutes,
    \"EFFORT DISTANCE KM\" as effort_distance_km,
    \"NUMBER OBSERVERS\" as number_observers,
    cast(\"ALL SPECIES REPORTED\" as boolean) as all_species_reported,
    \"GROUP IDENTIFIER\" as group_identifier,
    cast(\"APPROVED\" as boolean) as approved,
    cast(\"REVIEWED\" as boolean) as reviewed
FROM
    read_csv(
        '/dev/stdin',
        store_rejects = true,
    quote = '', 
    compression = 'gzip'
);
"

echo "  - Extracting ${EBD_NAME}.txt.gz from tar archive"

# Function to format bytes for human readability (macOS compatible)
format_bytes() {
    local bytes=$1
    if [ $bytes -ge 1073741824 ]; then
        echo "$(echo "scale=1; $bytes / 1073741824" | bc)G"
    elif [ $bytes -ge 1048576 ]; then
        echo "$(echo "scale=1; $bytes / 1048576" | bc)M"
    elif [ $bytes -ge 1024 ]; then
        echo "$(echo "scale=1; $bytes / 1024" | bc)K"
    else
        echo "${bytes}B"
    fi
}

# Get file sizes for progress calculation
TAR_SIZE=$(stat -f%z "${DIR_WITH_TAR}${EBD_NAME}.tar")
echo "  - Archive size: $(format_bytes ${TAR_SIZE})"

# Get compressed .gz file size from tar listing
GZ_SIZE=$(tar -tvf "${DIR_WITH_TAR}${EBD_NAME}.tar" | grep "${EBD_NAME}.txt.gz" | head -1 | awk '{print $5}')
if [ -n "$GZ_SIZE" ]; then
    echo "  - Compressed .gz file size: $(format_bytes ${GZ_SIZE})"
fi

# Check for pv availability (required)
if ! command -v pv >/dev/null 2>&1; then
    echo "Error: 'pv' (pipe viewer) is required but not installed"
    echo "Install with: brew install pv"
    exit 1
fi

echo "[$(date +'%H:%M:%S')] Beginning data extraction and processing..."
tar xfO ${DIR_WITH_TAR}${EBD_NAME}.tar ${EBD_NAME}.txt.gz | \
    pv -p -t -e -r -b -s "${GZ_SIZE}" | \
    gunzip -c | \
    duckdb ${OUTPUT_DB_PATH}${OUTPUT_DB_NAME} -c "${QUERY}" >/dev/null

echo "[$(date +'%H:%M:%S')] Data processing completed"

# Calculate and display execution time
END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))
ELAPSED_MINUTES=$((ELAPSED_TIME / 60))
ELAPSED_SECONDS=$((ELAPSED_TIME % 60))

echo "Processing completed at $(date)"
if [ ${ELAPSED_MINUTES} -gt 0 ]; then
    echo "Total execution time: ${ELAPSED_MINUTES}m ${ELAPSED_SECONDS}s"
else
    echo "Total execution time: ${ELAPSED_SECONDS}s"
fi

duckdb ${OUTPUT_DB_PATH}${OUTPUT_DB_NAME} <<EOF
describe ebd_full;
EOF

# Display database info
if [ -f "${OUTPUT_DB_PATH}${OUTPUT_DB_NAME}" ]; then
    DB_SIZE=$(stat -f%z "${OUTPUT_DB_PATH}${OUTPUT_DB_NAME}")
    echo "Database size: $(format_bytes ${DB_SIZE})"
    echo "GZ -> DB scale: $(echo "scale=2; ${DB_SIZE} / ${GZ_SIZE}" | bc)"
    echo "Database location: ${OUTPUT_DB_PATH}${OUTPUT_DB_NAME}"
fi
