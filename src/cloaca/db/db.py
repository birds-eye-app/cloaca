import duckdb
import os
from tabulate import tabulate


def get_db_connection_with_env(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    try:
        duck_db_path = os.environ["DUCK_DB_PATH"]
    except KeyError:
        raise RuntimeError(
            "DUCK_DB_PATH environment variable is required. "
            "Please set it to the path of your spatial database file."
        )

    return get_db_connection_with_path(duck_db_path, read_only=read_only)


def report_table_stats(con: duckdb.DuckDBPyConnection):
    try:
        # Get table statistics
        query = """
        SELECT
            database_name,
            table_name,
            estimated_size,
            column_count,
            index_count
            FROM
            duckdb_tables()
            where
            schema_name = 'main'
            and database_name not in ('_duckdb_ui', 'localmemdb');
        """
        stats = con.execute(query).fetchall()
        print("Table stats:")
        print(
            tabulate(
                stats,
                headers=["Database", "Table", "Rows", "Columns", "Indexes"],
                tablefmt="grid",
            )
        )
        indexes = con.execute("SELECT * FROM duckdb_indexes();").fetchall()
        print("indexes:\n\t", "\n\t".join([f"{row[6]}: {row[4]}" for row in indexes]))
    except Exception as e:
        print(f"Error reporting table stats: {e}")


def get_db_connection_with_path(
    duck_db_path: str,
    read_only: bool = True,
    verify_tables_exist=True,
    require_db_to_exist_already=True,
) -> duckdb.DuckDBPyConnection:
    if require_db_to_exist_already and not os.path.exists(duck_db_path):
        raise FileNotFoundError(f"Parsed DuckDB file not found at {duck_db_path}. ")

    print(f"Connecting to DuckDB at {duck_db_path} with read_only={read_only}")
    try:
        con = duckdb.connect(duck_db_path, read_only=read_only)
        # Load spatial extension
        con.install_extension("spatial")
        con.load_extension("spatial")

        if verify_tables_exist:
            report_table_stats(con)

        return con
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")
        raise e
