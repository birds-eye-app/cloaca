import argparse
import sys
import time
import threading
from pathlib import Path

from dotenv import load_dotenv
from duckdb import DuckDBPyConnection
import duckdb

from cloaca.db.db import get_db_connection_with_path, report_table_stats


load_dotenv()


class Spinner:
    """Simple spinner for showing progress during long operations."""

    def __init__(self, message="Working"):
        self.message = message
        self.spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.running = False
        self.thread = None

    def _spin(self):
        i = 0
        while self.running:
            print(
                f"\r  {self.message}... {self.spinner_chars[i % len(self.spinner_chars)]}",
                end="",
                flush=True,
            )
            time.sleep(0.1)
            i += 1

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._spin, daemon=True)
        self.thread.start()

    def stop(self, success_message="Done"):
        self.running = False
        if self.thread:
            self.thread.join(timeout=0.2)
        print(f"\r  {self.message}... ✅ {success_message}!     ")


def create_hotspot_popularity_table(con):
    start = time.time()
    spinner = Spinner("Creating hotspot popularity table")
    spinner.start()

    try:
        create_table_query = """
            create or replace table hotspot_popularity as (
                with number_of_weeks_in_each_month as (
                    select
                        extract(month from "OBSERVATION DATE") as month,
                        count(distinct date_trunc('week', "OBSERVATION DATE")) as num_weeks
                    from
                        ebd_full.full
                    where
                        "OBSERVATION DATE" > current_date - interval '5 year'
                    group by
                        1
                )
                select
                    CAST(SUBSTRING("LOCALITY ID", 2) AS INTEGER) as locality_id_int,
                    extract(month from "OBSERVATION DATE") as month,
                    -- I think i'm doing this right here?
                    count(distinct "SAMPLING EVENT IDENTIFIER") / max(num_weeks) as avg_weekly_number_of_observations
                from
                    ebd_full.full
                    join number_of_weeks_in_each_month on extract(month from "OBSERVATION DATE") = month
                where
                    "OBSERVATION DATE" > current_date - interval '5 year'
                group by
                    1,2
            )
        """
        con.execute(create_table_query)

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


def create_localities_table(con):
    start = time.time()
    spinner = Spinner("Creating localities table with spatial columns")
    spinner.start()

    try:
        create_table_query = """
            create or replace table localities as (
                select
                    distinct LOCALITY,
                    "LOCALITY ID" as locality_id,
                    CAST(SUBSTRING("LOCALITY ID", 2) AS INTEGER) as locality_id_int,
                    "LOCALITY TYPE" as locality_type,
                    LATITUDE,
                    LONGITUDE,
                    ST_Point(LATITUDE, LONGITUDE) as geometry
                from
                    ebd_full.full
                where
                    "OBSERVATION DATE" > current_date - interval '2 year'
                    and "LOCALITY TYPE" = 'H'
                    AND LATITUDE IS NOT NULL
                    AND LONGITUDE IS NOT NULL
                order by
                    locality_id
            )
        """
        con.execute(create_table_query)

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


def create_taxonomy_table(con, path_to_ebird_taxonomy_csv):
    start = time.time()
    spinner = Spinner("Creating taxonomy table")
    spinner.start()

    try:
        create_table_query = f"""
            create or replace table taxonomy as 
            select * from read_csv_auto('{path_to_ebird_taxonomy_csv}')
        """
        con.execute(create_table_query)

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


def setup_database_connections(input_ebd_db_path, parsed_output_db_path):
    print(f"Connecting to input EBD database: {input_ebd_db_path}")
    print(f"Connecting to parsed output database: {parsed_output_db_path}")

    con = get_db_connection_with_path(
        parsed_output_db_path,
        read_only=False,
        verify_tables_exist=False,
        require_db_to_exist_already=False,
    )

    con.execute(f"ATTACH DATABASE '{input_ebd_db_path}' AS ebd_full;")

    return con


def create_hotspots_richness_table(con: DuckDBPyConnection):
    start = time.time()
    spinner = Spinner("Creating hotspots richness table")
    spinner.start()

    try:
        query = """
            create or replace table hotspots_richness as (
                with
                    input_data as (
                    select distinct -- distinct is here to exclude duplicate (shared) checklists
                        extract (
                        month
                        from
                            "OBSERVATION DATE"
                        ) as month,
                        "LOCALITY ID" as locality_id,
                        "SAMPLING EVENT IDENTIFIER" as checklist_id,
                        "COMMON NAME" as common_name
                    from
                        ebd_full.full
                    where
                        "OBSERVATION DATE" > current_date - interval '5 year'
                        and CATEGORY = 'species'
                        and "PROTOCOL TYPE" in ('Stationary', 'Traveling')
                        and "EFFORT DISTANCE KM" < 10
                    ),
                    total_checklists as (
                    select
                        month,
                        locality_id,
                        count(distinct checklist_id) as total_checklists
                    from
                        input_data
                    group by
                        1,
                        2
                    ),
                    species_checklists as (
                    select
                        month,
                        locality_id,
                        common_name,
                        count(distinct checklist_id) as species_checklists
                    from
                        input_data
                    group by
                        1,
                        2,
                        3
                    ),
                    d as (
                    select
                        month,
                        locality_id,
                        count(distinct common_name) filter(
                        where
                            species_checklists / total_checklists > 0.06
                        ) as common_species,
                        count(distinct common_name) filter(
                        where
                            species_checklists / total_checklists between 0.01 and 0.06
                        ) as uncommon_species,
                        max(total_checklists) as total_checklists
                    from
                        total_checklists
                        join species_checklists using (locality_id, month)
                    group by
                        1,
                        2
                    )
                select
                    *,
                    common_species + uncommon_species as common_and_uncommon_species,
                    CAST(SUBSTRING(locality_id, 2) AS INTEGER) as locality_id_int,
                    -- Standard error approximation
                    sqrt(common_species) / total_checklists as std_error,
                    -- 95% confidence interval
                    common_species - 1.96 * sqrt(common_species) as ci_lower,
                    common_species + 1.96 * sqrt(common_species) as ci_upper
                from
                    d
                order by
                    std_error
                )
            """

        con.execute(query)
        print(con.description)

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")
    except duckdb.Error as e:
        print(f"DuckDB Error: {e}")
    except Exception as e:
        print("Error creating hotspots richness table:", e)


def drop_hotspots_richness_table(con):
    """Drop the hotspots richness table if it exists."""
    start = time.time()
    spinner = Spinner("Dropping hotspots richness table")
    spinner.start()

    try:
        con.execute("DROP TABLE IF EXISTS hotspots_richness")
        end = time.time()
        spinner.stop(f"Dropped in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


def create_localities_hotspots_table(con):
    """Create optimized merged table combining localities and hotspot data."""
    start = time.time()
    spinner = Spinner("Creating localities_hotspots optimization table")
    spinner.start()

    try:
        create_table_query = """
            CREATE OR REPLACE TABLE localities_hotspots AS
            SELECT
            l.locality_id,
            l.LOCALITY as locality_name,
            l.LATITUDE as latitude,
            l.LONGITUDE as longitude,
            l.locality_type,
            l.geometry,
            hp.month,
            hp.avg_weekly_number_of_observations,
            hr.common_species,
            hr.uncommon_species, 
            hr.common_and_uncommon_species,
            hr.std_error
            FROM
            localities l
            JOIN hotspot_popularity hp USING (locality_id_int)
            LEFT JOIN hotspots_richness hr using (locality_id_int)
            WHERE
            l.locality_type = 'H'
        """
        con.execute(create_table_query)

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


def build_parsed_db(args):
    # Validate input files exist
    ebd_path = Path(args.ebd_db)
    output_db_path = Path(args.output)
    taxonomy_path = Path(args.taxonomy)

    if not ebd_path.exists():
        print(f"Error: EBD database file not found: {ebd_path}")
        sys.exit(1)

    if not taxonomy_path.exists():
        print(f"Error: Taxonomy CSV file not found: {taxonomy_path}")
        sys.exit(1)

    print("=== Building Parsed eBird Database ===")
    print(f"Input EBD DB: {ebd_path}")
    print(f"Taxonomy CSV: {taxonomy_path}")

    total_start = time.time()
    con = None

    try:
        # Set up database connection
        con = setup_database_connections(ebd_path, output_db_path)

        # Create tables
        create_localities_table(con)
        create_taxonomy_table(con, str(taxonomy_path))
        create_hotspot_popularity_table(con)
        create_hotspots_richness_table(con)

        # Create optimized merged table for better query performance
        create_localities_hotspots_table(con)

        # drop this once we've created the final table for DB size
        drop_hotspots_richness_table(con)

        # Show statistics
        report_table_stats(con)

        total_end = time.time()
        print(
            f"\n=== Completed successfully in {total_end - total_start:.1f} seconds ==="
        )

        # Show output database path
        print(f"Parsed database created at: {output_db_path}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build parsed eBird database with weekly aggregations",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-e", "--ebd-db", required=True, help="Path to the full eBird DuckDB file"
    )

    parser.add_argument(
        "-t", "--taxonomy", required=True, help="Path to the eBird taxonomy CSV file"
    )

    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip creating indexes (faster for development)",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Output path for the parsed database file",
    )

    args = parser.parse_args()
    build_parsed_db(args)
