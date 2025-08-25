import duckdb
import time
from typing import List, Dict, Any


class PopularHotspotResult:
    def __init__(
        self,
        locality_id: str,
        locality_name: str,
        latitude: float,
        longitude: float,
        avg_weekly_checklists: float,
    ):
        self.locality_id = locality_id
        self.locality_name = locality_name
        self.latitude = latitude
        self.longitude = longitude
        self.avg_weekly_checklists = avg_weekly_checklists

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "locality_id": self.locality_id,
            "locality_name": self.locality_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "avg_weekly_checklists": self.avg_weekly_checklists,
        }
        return result


def get_db_connection():
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

    con = duckdb.connect(duck_db_path)

    try:
        # Load spatial extension
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Verify spatial columns exist
        con.execute("SELECT geometry FROM localities LIMIT 1")

        return con

    except Exception as e:
        con.close()
        raise RuntimeError(f"DB file found but spatial extension not working: {e}. ")


def get_popular_hotspots(
    latitude: float, longitude: float, radius_km: float, month: int
) -> List[PopularHotspotResult]:
    con = get_db_connection()

    try:
        query = """
        SELECT 
            localities.locality_id,
            LOCALITY as locality_name,
            LATITUDE as latitude,
            LONGITUDE as longitude,
            avg_weekly_number_of_observations
        FROM localities
        JOIN hotspot_popularity ON localities.locality_id_int = hotspot_popularity.locality_id_int
        WHERE locality_type = 'H'  -- Only hotspots
            AND ST_Distance_Sphere(geometry, ST_Point(?, ?)) <= ?  -- Great circle distance in meters
            AND hotspot_popularity.month = ?
            and hotspot_popularity.avg_weekly_number_of_observations >= 1
        """

        print(
            f"Executing get_popular_hotspots with lat: {latitude}, lon: {longitude}, radius: {radius_km}km, month: {month}"
        )

        # Convert km to meters for the query
        radius_meters = radius_km * 1000
        # Note: ST_Distance_Sphere expects [latitude, longitude] axis order per docs
        params = [latitude, longitude, radius_meters, month]
        query_start_time = time.time()
        result = con.execute(query, params).fetchall()
        query_end_time = time.time()
        print(
            f"[DuckDB Spatial] Query execution took {query_end_time - query_start_time:.3f}s, returned {len(result)} rows"
        )

        # Convert to objects
        hotspots = [
            PopularHotspotResult(
                locality_id=row[0],
                locality_name=row[1],
                latitude=row[2],
                longitude=row[3],
                avg_weekly_checklists=row[4],
            )
            for row in result
        ]

        conversion_time = time.time() - query_end_time
        print(f"[DuckDB Spatial] Result conversion took {conversion_time:.3f}s")

    except Exception as e:
        print(f"[DuckDB Spatial] Error occurred: {e}")
        return []

    return hotspots
