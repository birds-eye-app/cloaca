import duckdb
import time
import math
from typing import List, Dict, Any


class PopularHotspotResult:
    def __init__(
        self,
        locality_id: str,
        locality_name: str,
        locality_type: str,
        latitude: float,
        longitude: float,
        avg_weekly_checklists: float,
        country_code: str,
        state_code: str,
        county_code: str,
        distance_km: float | None = None,
    ):
        self.locality_id = locality_id
        self.locality_name = locality_name
        self.locality_type = locality_type
        self.latitude = latitude
        self.longitude = longitude
        self.avg_weekly_checklists = avg_weekly_checklists
        self.country_code = country_code
        self.state_code = state_code
        self.county_code = county_code
        self.distance_km = distance_km

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "locality_id": self.locality_id,
            "locality_name": self.locality_name,
            "locality_type": self.locality_type,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "avg_weekly_checklists": self.avg_weekly_checklists,
            "country_code": self.country_code,
            "state_code": self.state_code,
            "county_code": self.county_code,
        }
        if self.distance_km is not None:
            result["distance_km"] = self.distance_km
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
        raise FileNotFoundError(
            f"Spatial database not found at {duck_db_path}. "
            "Please run 'python src/cloaca/db/build_parsed_db.py' to create the spatial database."
        )

    con = duckdb.connect(duck_db_path)

    try:
        # Load spatial extension
        con.execute("LOAD spatial")

        # Verify spatial columns exist
        con.execute("SELECT geometry FROM localities LIMIT 1")

        return con

    except Exception as e:
        con.close()
        raise RuntimeError(
            f"Spatial database found but spatial extension not working: {e}. "
            "Please rebuild the spatial database with 'python src/cloaca/db/build_spatial_db.py'"
        )


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points on Earth in km."""
    import math

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    # Radius of Earth in kilometers
    r = 6371

    return c * r


def get_bounding_box(lat: float, lng: float, radius_km: float) -> dict:
    """Calculate bounding box for approximate geographic filtering"""
    # Rough approximation: 1 degree lat ≈ 111km
    lat_delta = radius_km / 111.0
    # Longitude delta varies by latitude
    lng_delta = radius_km / (111.0 * abs(math.cos(math.radians(lat))))

    return {
        "min_lat": lat - lat_delta,
        "max_lat": lat + lat_delta,
        "min_lng": lng - lng_delta,
        "max_lng": lng + lng_delta,
    }


def get_popular_hotspots(
    latitude: float, longitude: float, radius_km: float, month: int
) -> List[PopularHotspotResult]:
    """
    Get popular hotspots using spatial extension for optimized geographic queries.

    Args:
        latitude: Center latitude
        longitude: Center longitude
        radius_km: Search radius in kilometers
        month: Month number (1-12)

    Returns:
        List of PopularHotspotResult objects sorted by avg_weekly_checklists descending
    """
    print(
        f"[DuckDB Spatial] Starting spatial popular hotspots query for lat={latitude}, lng={longitude}, radius={radius_km}km, month={month}"
    )
    start_time = time.time()

    con = get_db_connection()
    db_connect_time = time.time()
    print(
        f"[DuckDB Spatial] Database connection took {db_connect_time - start_time:.3f}s"
    )

    try:
        # Spatial query using ST_DWithin for efficient radius filtering
        query = """
        WITH spatial_localities AS (
            SELECT 
                locality_id,
                LOCALITY as locality_name,
                locality_type,
                LATITUDE as latitude,
                LONGITUDE as longitude,
                country_code,
                state_code,
                county_code,
                ST_Distance(geometry, ST_Point(?, ?)) * 100.0 as distance_km
            FROM localities
            WHERE locality_type = 'H'  -- Only hotspots
              AND ST_DWithin(geometry, ST_Point(?, ?), ?)  -- Spatial radius filter
        ),
        monthly_observations AS (
            SELECT 
                locality_id,
                sum(number_of_checklists) / count(distinct week) as avg_weekly_checklists
            FROM weekly_species_observations
            WHERE EXTRACT(MONTH FROM week) = ?
            GROUP BY locality_id
            HAVING COUNT(*) > 0  -- Ensure we have data
        )
        SELECT 
            l.locality_id,
            l.locality_name,
            l.locality_type,
            l.latitude,
            l.longitude,
            o.avg_weekly_checklists,
            l.country_code,
            l.state_code,
            l.county_code,
            l.distance_km
        FROM spatial_localities l
        INNER JOIN monthly_observations o ON l.locality_id = o.locality_id
        ORDER BY o.avg_weekly_checklists DESC
        """

        # Execute spatial query
        query_start = time.time()
        radius_m = radius_km / 100  # Convert km to meters for ST_DWithin
        params = [longitude, latitude, longitude, latitude, radius_m, month]
        result = con.execute(query, params).fetchall()
        query_end = time.time()
        print(
            f"[DuckDB Spatial] Spatial query execution took {query_end - query_start:.3f}s, returned {len(result)} rows"
        )

        # Convert to objects (no additional distance filtering needed!)
        conversion_start = time.time()
        hotspots = []

        for row in result:
            hotspots.append(
                PopularHotspotResult(
                    locality_id=row[0],
                    locality_name=row[1],
                    locality_type=row[2],
                    latitude=row[3],
                    longitude=row[4],
                    avg_weekly_checklists=float(row[5]),
                    country_code=row[6],
                    state_code=row[7],
                    county_code=row[8],
                    distance_km=float(row[9]),
                )
            )

        conversion_end = time.time()
        print(
            f"[DuckDB Spatial] Object conversion took {conversion_end - conversion_start:.3f}s"
        )
        print(f"[DuckDB Spatial] Found {len(hotspots)} hotspots within {radius_km}km")

        # Already sorted by query
        total_time = time.time() - start_time
        print(f"[DuckDB Spatial] Total spatial query completed in {total_time:.3f}s")

        return hotspots

    finally:
        con.close()
