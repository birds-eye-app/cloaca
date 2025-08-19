import argparse
import duckdb
import sys
import time
from pathlib import Path


def create_weekly_species_observations_table(con):
    print("Creating weekly_species_observations table...")
    start = time.time()
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
    print(f"Created weekly_species_observations table in {end - start:.3f} seconds")


# create table parsed_ebd.localities as (
#     select
#         distinct "COUNTRY CODE" as country_code,
#         "STATE CODE" as state_code,
#         "COUNTY CODE" as county_code,
#         LOCALITY,
#         "LOCALITY ID" as locality_id,
#         "LOCALITY TYPE" as locality_type,
#         latitude,
#         LONGITUDE
#     from
#         ebd_full."full"
#     where
#         "OBSERVATION DATE" > current_date - interval '5 year'
#     order by
#         locality_id
# )


def create_localities_table(con):
    print("Creating localities table...")
    start = time.time()
    create_table_query = """
        create or replace table localities as (
            select
                distinct "COUNTRY CODE" as country_code,
                "STATE CODE" as state_code,
                "COUNTY CODE" as county_code,
                LOCALITY,
                "LOCALITY ID" as locality_id,
                "LOCALITY TYPE" as locality_type,
                latitude,
                LONGITUDE
            from
                ebd_full."full"
            where
                "OBSERVATION DATE" > current_date - interval '5 year'
            order by
                locality_id
        )
    """
    con.execute(create_table_query)

    end = time.time()
    print(f"Created localities table in {end - start:.3f} seconds")


def create_taxonomy_table(con, path_to_ebird_taxonomy_csv):
    print("Creating taxonomy table...")
    start = time.time()

    # First read the CSV to understand its structure
    print(f"Reading taxonomy CSV from: {path_to_ebird_taxonomy_csv}")
    create_table_query = f"""
        create or replace table taxonomy as 
        select * from read_csv_auto('{path_to_ebird_taxonomy_csv}')
    """
    con.execute(create_table_query)

    end = time.time()
    print(f"Created taxonomy table in {end - start:.3f} seconds")


def setup_database_connection(ebd_db_path):
    """Set up database connection and attach the EBD database."""
    print(f"Connecting to EBD database: {ebd_db_path}")

    # Create a new database for parsed results
    parsed_db_path = Path(ebd_db_path).parent / "parsed_ebd.db"
    print(f"Creating parsed database: {parsed_db_path}")

    con = duckdb.connect(str(parsed_db_path))

    # Attach the full EBD database
    con.execute(f"ATTACH '{ebd_db_path}' AS ebd_full")

    return con


def get_table_stats(con):
    """Get statistics about the created tables."""
    print("\n=== Database Statistics ===")

    tables = ["weekly_species_observations", "localities", "taxonomy"]

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

    parser.add_argument("ebd_db_path", help="Path to the full eBird DuckDB file")

    parser.add_argument("taxonomy_csv_path", help="Path to the eBird taxonomy CSV file")

    parser.add_argument(
        "--output-dir",
        help="Directory to create parsed database in (default: same as input)",
    )

    args = parser.parse_args()

    # Validate input files exist
    ebd_path = Path(args.ebd_db_path)
    taxonomy_path = Path(args.taxonomy_csv_path)

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
        con = setup_database_connection(ebd_path)

        # Create tables
        create_weekly_species_observations_table(con)
        create_localities_table(con)
        create_taxonomy_table(con, str(taxonomy_path))

        # Show statistics
        get_table_stats(con)

        total_end = time.time()
        print(
            f"\n=== Completed successfully in {total_end - total_start:.1f} seconds ==="
        )

        # Show output database path
        parsed_db_path = Path(ebd_path).parent / "parsed_ebd.db"
        print(f"Parsed database created at: {parsed_db_path}")

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
