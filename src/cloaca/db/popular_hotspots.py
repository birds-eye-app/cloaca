import time
from typing import List, Dict, Any

import duckdb


class PopularHotspotResult:
    def __init__(
        self,
        locality_id: str,
        locality_name: str,
        latitude: float,
        longitude: float,
        avg_weekly_checklists: float,
        likely_common_species_count: int,
        likely_common_species_std_error: float,
        likely_uncommon_species_count: int,
        likely_common_and_uncommon_species_count: int,
    ):
        self.locality_id = locality_id
        self.locality_name = locality_name
        self.latitude = latitude
        self.longitude = longitude
        self.avg_weekly_checklists = avg_weekly_checklists
        self.likely_common_species_count = likely_common_species_count
        self.likely_common_species_std_error = likely_common_species_std_error
        self.likely_uncommon_species_count = likely_uncommon_species_count
        self.likely_common_and_uncommon_species_count = (
            likely_common_and_uncommon_species_count
        )

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "locality_id": self.locality_id,
            "locality_name": self.locality_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "avg_weekly_checklists": self.avg_weekly_checklists,
            "likely_common_species_count": self.likely_common_species_count,
            "likely_common_species_std_error": self.likely_common_species_std_error,
            "likely_uncommon_species_count": self.likely_uncommon_species_count,
            "likely_common_and_uncommon_species_count": self.likely_common_and_uncommon_species_count,
        }
        return result


def get_popular_hotspots(
    con: duckdb.DuckDBPyConnection,
    latitude: float,
    longitude: float,
    radius_km: float,
    month: int,
) -> List[PopularHotspotResult]:
    try:
        query = """
        SELECT 
            locality_id,
            locality_name,
            latitude,
            longitude,
            avg_weekly_number_of_observations,
            common_species as likely_common_species_count,
            std_error as likely_common_species_std_error,
            uncommon_species as likely_uncommon_species_count,
            common_and_uncommon_species as likely_common_and_uncommon_species_count
        FROM localities_hotspots
        WHERE ST_Distance_Sphere(geometry, ST_Point(?, ?)) <= ?  -- Great circle distance in meters
            AND month = ?
            AND avg_weekly_number_of_observations >= 1
        ORDER BY avg_weekly_number_of_observations DESC
        limit 1000
        """

        print(
            f"Executing optimized get_popular_hotspots with lat: {latitude}, lon: {longitude}, radius: {radius_km}km, month: {month}"
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
                likely_common_species_count=row[5],
                likely_common_species_std_error=row[6],
                likely_uncommon_species_count=row[7],
                likely_common_and_uncommon_species_count=row[8],
            )
            for row in result
        ]

        conversion_time = time.time() - query_end_time
        print(f"[DuckDB Spatial] Result conversion took {conversion_time:.3f}s")

    except Exception as e:
        print(f"[DuckDB Spatial] Error occurred: {e}")
        return []

    return hotspots
