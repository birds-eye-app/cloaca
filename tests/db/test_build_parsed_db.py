import argparse
from duckdb import DuckDBPyConnection
import pytest
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from cloaca.db.build_parsed_db import (
    build_parsed_db,
    setup_database_connections,
    create_localities_table,
    create_taxonomy_table,
    create_hotspot_popularity_table,
    create_hotspots_richness_table,
    create_localities_hotspots_table,
)
from cloaca.db.db import get_db_connection_with_path


def assert_expected_vs_actual_columns(
    con: DuckDBPyConnection, expected_columns: list[str], table_name: str
):
    """Assert that the expected columns match the actual columns."""

    column_query = """
    SELECT
        column_name
    FROM
        information_schema.columns
    WHERE
        table_name = ?;
    """

    actual_columns = (
        con.execute(column_query, (table_name,)).fetchdf().column_name.tolist()
    )
    if sorted(expected_columns) == sorted(actual_columns):
        return

    missing_columns = set(expected_columns) - set(actual_columns)
    assert not missing_columns, f"Missing columns: {missing_columns}"


class TestBuildParsedDb:
    """Test the build_parsed_db script functionality."""

    @pytest.fixture
    def test_input_db(self):
        """Path to the test input database with 100 rows."""
        return Path(__file__).parent.parent / "data" / "test_input.db"

    @pytest.fixture
    def taxonomy_csv(self):
        """Path to the eBird taxonomy CSV file."""
        return Path(__file__).parent.parent / "data" / "eBird_taxonomy_v2024.csv"

    @pytest.fixture
    def test_output_parsed_db(self):
        """Path to the test output database."""
        return Path(__file__).parent.parent / "data" / "test_parsed.db"

    def test_input_database_exists(self, test_input_db):
        """Test that the input test database exists and has the expected structure."""
        assert test_input_db.exists(), "Test input database should exist"

        # Connect and verify structure
        con = get_db_connection_with_path(
            str(test_input_db), read_only=True, verify_tables_exist=False
        )

        # Check row count
        row_count = con.execute("SELECT count(*) FROM ebd_sorted").fetchone()
        assert row_count is not None and row_count[0] == 4863, (
            f"Expected 4863 rows, got {row_count}"
        )

        assert_expected_vs_actual_columns(
            con,
            [
                "observation_date",
                "locality",
                "locality_id",
                "category",
                "sampling_event_identifier",
                "locality_type",
                "latitude",
                "longitude",
                "common_name",
                "protocol_name",
                "effort_distance_km",
            ],
            "ebd_sorted",
        )

        con.close()

    def test_build_parsed_db_complete_pipeline(
        self, test_input_db, taxonomy_csv, test_output_parsed_db
    ):
        """Test the complete build_parsed_db pipeline with sample data."""
        args = argparse.Namespace(
            ebd_db=str(test_input_db),
            taxonomy=str(taxonomy_csv),
            skip_indexes=False,
            output=str(test_output_parsed_db),
        )

        build_parsed_db(args)

        con = setup_database_connections(str(test_input_db), test_output_parsed_db)
        try:
            # Test that all expected tables were created
            tables = con.execute("SHOW TABLES").fetchall()
            expected_tables = [
                "localities",
                "taxonomy",
                "hotspot_popularity",
                "localities_hotspots",
            ]

            assert set(expected_tables).issubset(set(row[0] for row in tables))

        finally:
            con.close()

    def test_localities_table_structure(self, test_input_db, test_output_parsed_db):
        """Test the structure of the localities table."""
        con = setup_database_connections(str(test_input_db), test_output_parsed_db)

        try:
            create_localities_table(con)

            # Check columns and ensure table has data
            assert_expected_vs_actual_columns(
                con,
                [
                    "locality",
                    "locality_id",
                    "locality_id_int",
                    "locality_type",
                    "latitude",
                    "longitude",
                    "geometry",
                ],
                "localities",
            )

            result = con.query("SELECT * FROM localities LIMIT 1").fetchall()
            assert len(result) > 0, "localities table should have data"

        finally:
            con.close()

    def test_hotspot_popularity_table_structure(
        self, test_input_db, test_output_parsed_db
    ):
        """Test the structure of the hotspot_popularity table."""
        con = setup_database_connections(str(test_input_db), test_output_parsed_db)

        try:
            create_hotspot_popularity_table(con)

            assert_expected_vs_actual_columns(
                con,
                [
                    "locality_id_int",
                    "month",
                    "avg_weekly_number_of_observations",
                ],
                "hotspot_popularity",
            )
            result = con.query("SELECT * FROM hotspot_popularity").fetchall()

            assert len(result) > 1, "hotspot_popularity table should have data"

        finally:
            con.close()

    def test_taxonomy_table_creation(
        self, test_input_db, taxonomy_csv, test_output_parsed_db
    ):
        """Test that the taxonomy table is created correctly."""
        con = setup_database_connections(str(test_input_db), test_output_parsed_db)

        try:
            create_taxonomy_table(con, str(taxonomy_csv))

            # Check it has data
            row_count = con.execute("SELECT count(*) FROM taxonomy").fetchone()
            assert row_count is not None and row_count[0] > 0, (
                "Taxonomy table should have data"
            )

        finally:
            con.close()

    def test_localities_hotspots_merged_table(
        self, test_input_db, test_output_parsed_db
    ):
        """Test the final merged localities_hotspots table."""
        con = setup_database_connections(str(test_input_db), test_output_parsed_db)

        try:
            # Run the full pipeline
            create_localities_table(con)
            create_hotspot_popularity_table(con)
            create_hotspots_richness_table(con)
            create_localities_hotspots_table(con)

            # Check columns and ensure table has data
            assert_expected_vs_actual_columns(
                con,
                [
                    "locality_id",
                    "locality_name",
                    "latitude",
                    "longitude",
                    "locality_type",
                    "geometry",
                    "month",
                    "avg_weekly_number_of_observations",
                ],
                "localities_hotspots",
            )
            result = con.query("SELECT * FROM localities_hotspots LIMIT 1").fetchall()

            assert len(result) > 0, "localities_hotspots table should have data"

        finally:
            con.close()
