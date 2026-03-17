#!/bin/bash
set -e

usage() {
    echo "Usage: $0 --source <source.db> --output <output.db> [--limit N]"
    echo ""
    echo "  --source  Path to full EBD source DB (default: /Volumes/lacie_disk/ebd_relJan-2026.db)"
    echo "  --output  Path to write NYC DB (default: src/cloaca/swan_lake/dbs/ebd_nyc.db)"
    echo "  --limit   Row limit for testing (skips ORDER BY)"
    exit 1
}

SOURCE="/Volumes/lacie_disk/ebd_relJan-2026.db"
OUTPUT="src/cloaca/swan_lake/dbs/ebd_nyc.db"
LIMIT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --source) SOURCE="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        --limit)  LIMIT="$2"; shift 2 ;;
        *) usage ;;
    esac
done

if [ ! -f "$SOURCE" ]; then
    echo "Error: source DB not found: $SOURCE"
    exit 1
fi

if [ -f "$OUTPUT" ]; then
    echo "Error: output DB already exists: $OUTPUT"
    echo "Delete it first if you want to rebuild."
    exit 1
fi

ORDER_BY="ORDER BY locality_id, observation_date"
LIMIT_CLAUSE=""
if [ -n "$LIMIT" ]; then
    ORDER_BY=""
    LIMIT_CLAUSE="LIMIT $LIMIT"
    echo "Mode:   test (limit $LIMIT rows, ORDER BY skipped)"
fi

echo "Source: $SOURCE"
echo "Output: $OUTPUT"
echo ""

SETUP_SQL="
CREATE TYPE IF NOT EXISTS category_t      AS ENUM ('species','issf','spuh','slash','hybrid','form','domestic','intergrade');
CREATE TYPE IF NOT EXISTS exotic_code_t   AS ENUM ('N','X','P');
CREATE TYPE IF NOT EXISTS locality_type_t AS ENUM ('C','H','P','PC','S','T');
CREATE TYPE IF NOT EXISTS protocol_t      AS ENUM ('Area','Banding','Breeding Bird Atlas','Historical','Incidental','My Yard Counts','Nocturnal Flight Call Count','Random','Stationary','Stationary (2 band, 100m)','Stationary (2 band, 25m)','Stationary (2 band, 30m)','Stationary (2 band, 50m)','Stationary (2 band, 75m)','Stationary (3 band, 30m+100m)','Stationary (Directional)','Traveling','Traveling (2 band, 25m)','Traveling - Property Specific','eBird Pelagic Protocol');
"


CREATE_SQL="
CREATE TABLE ebd_nyc AS
SELECT
    category::category_t                    AS category,
    common_name,
    scientific_name,
    subspecies_common_name,
    subspecies_scientific_name,
    exotic_code::exotic_code_t              AS exotic_code,
    observation_count,
    country_code,
    state_code,
    county_code,
    locality,
    locality_id,
    locality_type::locality_type_t          AS locality_type,
    latitude::FLOAT                         AS latitude,
    longitude::FLOAT                        AS longitude,
    observation_date,
    sampling_event_identifier,
    protocol_name::protocol_t               AS protocol_name,
    duration_minutes::INTEGER               AS duration_minutes,
    effort_distance_km::FLOAT               AS effort_distance_km,
    number_observers::SMALLINT              AS number_observers,
    all_species_reported,
    group_identifier,
    approved,
    reviewed
FROM src.ebd_full
WHERE county_code IN ('US-NY-061','US-NY-047','US-NY-081','US-NY-005','US-NY-085')
$ORDER_BY
$LIMIT_CLAUSE
"

INIT_FILE=$(mktemp /tmp/duckdb_init.XXXXXX.sql)
cat > "$INIT_FILE" <<EOF
ATTACH '$SOURCE' AS src (READ_ONLY);
SET enable_progress_bar = true;
EOF

START=$(date +%s)
duckdb "$OUTPUT" -c "$SETUP_SQL"
if ! duckdb "$OUTPUT" -init "$INIT_FILE" -c "$CREATE_SQL"; then
    echo "Error: duckdb failed"
    rm -f "$OUTPUT" "$INIT_FILE"
    exit 1
fi
rm -f "$INIT_FILE"

duckdb "$OUTPUT" -c "SELECT COUNT(*) AS row_count FROM ebd_nyc;"

END=$(date +%s)
ELAPSED=$(( (END - START) / 60 ))
SIZE=$(du -sh "$OUTPUT" | cut -f1)

echo ""
echo "Done in ${ELAPSED} minutes"
echo "DB size: $SIZE"
