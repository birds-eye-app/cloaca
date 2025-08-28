import duckdb


def get_db_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Get database connection with spatial support required."""
    import os

    try:
        duck_db_path = os.environ["DUCK_DB_PATH"]
    except KeyError:
        raise RuntimeError(
            "DUCK_DB_PATH environment variable is required. "
            "Please set it to the path of your spatial database file."
        )

    if not os.path.exists(duck_db_path):
        raise FileNotFoundError(f"Parsed DuckDB file not found at {duck_db_path}. ")

    con = duckdb.connect(duck_db_path, read_only=read_only)

    try:
        # Load spatial extension
        con.install_extension("spatial")
        con.load_extension("spatial")

        # verify tables and indexes that exist
        results = con.execute("SELECT COUNT(*) FROM localities_hotspots;").fetchall()
        print("localities_hotspots count:", results[0][0])
        indexes = con.execute("SELECT * FROM duckdb_indexes();").fetchall()
        for index in indexes:
            print("localities_hotspots index:", index)

        return con
    except Exception as e:
        con.close()
        raise RuntimeError(f"DB file found but spatial extension not working: {e}. ")
