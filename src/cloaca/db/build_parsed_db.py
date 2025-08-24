import argparse
import duckdb
import sys
import time
import threading
from pathlib import Path


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


def create_weekly_species_observations_table(con):
    start = time.time()
    spinner = Spinner("Creating weekly_species_observations table")
    spinner.start()

    try:
        create_table_query = """
            create or replace table weekly_species_observations as (
                select
                    coalesce("SUBSPECIES SCIENTIFIC NAME", "SCIENTIFIC NAME") as species_id,
                    date_trunc('week', "OBSERVATION DATE") as week,
                    "LOCALITY ID" as locality_id,
                    "EXOTIC CODE" as exotic_code,
                    count(*) as number_of_checklists -- sum("OBSERVATION COUNT") as number_of_observations
                from
                    ebd_full."full"
                where
                    "OBSERVATION DATE" > current_date - interval '5 years'
                group by
                    1,
                    2,
                    3,
                    4
            )
        """
        con.execute(create_table_query)

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


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
                        ebd_full."full"
                    where
                        "OBSERVATION DATE" > current_date - interval '5 year'
                    group by
                        1
                )
                select
                    "LOCALITY ID" as locality_id,
                    extract(month from "OBSERVATION DATE") as month,
                    -- I think i'm doing this right here?
                    count(*) / max(num_weeks) as avg_weekly_number_of_observations
                from
                    ebd_full."full"
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
                    "LOCALITY TYPE" as locality_type,
                    LATITUDE,
                    LONGITUDE,
                    ST_Point(LATITUDE, LONGITUDE) as geometry
                from
                    ebd_full."full"
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


def setup_database_connection(ebd_db_path, output_path=None):
    """Set up database connection with spatial extension."""
    print(f"Connecting to EBD database: {ebd_db_path}")

    # Create a new database for parsed results
    if output_path:
        parsed_db_path = Path(output_path)
        # Ensure the directory exists
        parsed_db_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        parsed_db_path = Path(ebd_db_path).parent / "parsed_ebd.db"

    print(f"Creating parsed database with spatial support: {parsed_db_path}")

    con = duckdb.connect(str(parsed_db_path))

    # Install and load spatial extension
    spinner = Spinner("Loading spatial extension")
    spinner.start()
    try:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        spinner.stop("Loaded")
    except Exception as e:
        spinner.stop("Failed")
        raise e

    # Attach the full EBD database
    con.execute(f"ATTACH '{ebd_db_path}' AS ebd_full")

    return con, parsed_db_path


def create_spatial_indexes(con):
    """Create spatial indexes for better query performance."""
    start = time.time()
    spinner = Spinner("Creating spatial and database indexes")
    spinner.start()

    try:
        # Create spatial index on localities geometry
        con.execute(
            "CREATE INDEX idx_localities_spatial ON localities USING RTREE (geometry)"
        )

        end = time.time()
        spinner.stop(f"Created in {end - start:.1f}s")

    except Exception as e:
        spinner.stop("Failed")
        raise e


def get_table_stats(con):
    """Get statistics about the created tables."""
    print("\n=== Database Statistics ===")

    tables = ["localities", "taxonomy", "hotspot_popularity"]

    for table in tables:
        try:
            count_result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"{table}: {count_result[0]:,} rows")

            # Show sample data
            sample = con.execute(f"SELECT * FROM {table} LIMIT 2").fetchall()
            if sample:
                print(f"  Sample: {sample[0]}")

        except Exception as e:
            print(f"Could not get stats for {table}: {e}")


def main():
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
        help="Output path for the parsed database file (default: same directory as input with name 'parsed_ebd.db')",
    )

    args = parser.parse_args()

    # Validate input files exist
    ebd_path = Path(args.ebd_db)
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

    try:
        # Set up database connection
        con, output_db_path = setup_database_connection(ebd_path, args.output)

        # Create tables
        # create_weekly_species_observations_table(con)
        create_localities_table(con)
        create_taxonomy_table(con, str(taxonomy_path))
        create_hotspot_popularity_table(con)

        # Create indexes unless skipped
        if not args.skip_indexes:
            create_spatial_indexes(con)
        else:
            print("Skipping index creation")

        # Show statistics
        get_table_stats(con)

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
        if "con" in locals():
            con.close()


if __name__ == "__main__":
    main()
