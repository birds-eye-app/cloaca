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

    def to_dict(self) -> Dict[str, Any]:
        return {
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


def get_db_connection():
    return duckdb.connect(
        "/Users/davidmeadows_1/programs/birds-eye-app/swan-lake/dbs/parsed_ebd.db"
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
    Get popular hotspots within a given radius and month.

    Args:
        latitude: Center latitude
        longitude: Center longitude
        radius_km: Search radius in kilometers
        month: Month number (1-12)

    Returns:
        List of PopularHotspotResult objects sorted by avg_weekly_checklists descending
    """
    print(
        f"[DuckDB] Starting popular hotspots query for lat={latitude}, lng={longitude}, radius={radius_km}km, month={month}"
    )
    start_time = time.time()

    # Calculate bounding box for pre-filtering (add 50% buffer for safety)
    bbox = get_bounding_box(latitude, longitude, radius_km * 1.5)

    con = get_db_connection()
    db_connect_time = time.time()
    print(f"[DuckDB] Database connection took {db_connect_time - start_time:.3f}s")

    try:
        # Optimized query with geographic bounding box pre-filtering
        query = """
        WITH filtered_localities AS (
            SELECT 
                locality_id,
                LOCALITY as locality_name,
                locality_type,
                LATITUDE as latitude,
                LONGITUDE as longitude,
                country_code,
                state_code,
                county_code
            FROM localities
            WHERE locality_type = 'H'  -- Only hotspots
              AND LATITUDE BETWEEN ? AND ?
              AND LONGITUDE BETWEEN ? AND ?
        ),
        monthly_observations AS (
            SELECT 
                locality_id,
                AVG(number_of_checklists) as avg_weekly_checklists
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
            l.county_code
        FROM filtered_localities l
        INNER JOIN monthly_observations o ON l.locality_id = o.locality_id
        ORDER BY o.avg_weekly_checklists DESC
        """

        # Execute query with bounding box parameters
        query_start = time.time()
        params = [
            bbox["min_lat"],
            bbox["max_lat"],
            bbox["min_lng"],
            bbox["max_lng"],
            month,
        ]
        result = con.execute(query, params).fetchall()
        query_end = time.time()
        print(
            f"[DuckDB] Query execution took {query_end - query_start:.3f}s, returned {len(result)} rows"
        )

        # Filter by distance and convert to objects
        distance_filter_start = time.time()
        hotspots = []
        distance_calculations = 0

        for row in result:
            hotspot_lat, hotspot_lon = row[3], row[4]
            distance = haversine_distance_km(
                latitude, longitude, hotspot_lat, hotspot_lon
            )
            distance_calculations += 1

            if distance <= radius_km:
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
                    )
                )

        distance_filter_end = time.time()
        print(
            f"[DuckDB] Distance filtering took {distance_filter_end - distance_filter_start:.3f}s"
        )
        print(
            f"[DuckDB] Processed {distance_calculations} distance calculations, filtered to {len(hotspots)} within {radius_km}km"
        )

        # Already sorted by query, no additional sorting needed

        total_time = time.time() - start_time
        print(f"[DuckDB] Total popular hotspots query completed in {total_time:.3f}s")

        return hotspots

    finally:
        con.close()
