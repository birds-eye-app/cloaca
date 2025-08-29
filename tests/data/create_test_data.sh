#!/bin/bash

# Script to create test data database for build_parsed_db tests
# This creates a small sample database with 100 rows from the real ebird_ny.db

set -e  # Exit on any error

# Configuration
# source db is first arg
SOURCE_DB="$1"
TEST_DATA_DIR="tests/data"
TEST_INPUT_DB="$TEST_DATA_DIR/test_input.db"

echo "=== Creating Test Data for build_parsed_db Tests ==="

# Create test data directory if it doesn't exist
echo "Creating test data directory..."
mkdir -p "$TEST_DATA_DIR"

# Remove existing test database if it exists
if [ -f "$TEST_INPUT_DB" ]; then
    echo "Removing existing test input database..."
    rm -f "$TEST_INPUT_DB"
fi

# Check if source database exists
if [ ! -f "$SOURCE_DB" ]; then
    echo "Error: Source database not found at $SOURCE_DB"
    exit 1
fi

echo "Creating test input database with 100 sample rows..."
duckdb "$TEST_INPUT_DB" <<SQL
ATTACH '$SOURCE_DB' AS source;
CREATE TABLE test_input.full AS (
    SELECT * FROM source.full 
    where
        "OBSERVATION DATE" > current_date - interval '2 year'
        and CATEGORY = 'species'
        and "PROTOCOL TYPE" in ('Stationary', 'Traveling')
        and "EFFORT DISTANCE KM" < 10
        and "LOCALITY TYPE" = 'H'
        AND LATITUDE IS NOT NULL
        AND LONGITUDE IS NOT NULL
    LIMIT 10000
);
DETACH source;
SQL

# Verify the test database was created correctly
echo "Verifying test database..."
duckdb "$TEST_INPUT_DB" "SELECT * FROM test_input.full;"

echo ""
echo "=== Test Data Creation Complete ==="
echo "Test input database: $TEST_INPUT_DB"