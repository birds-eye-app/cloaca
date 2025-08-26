from typing import List, Dict, Any

import duckdb
from cloaca.db.popular_hotspots import get_popular_hotspots


async def get_popular_hotspots_api(
    duck_db_conn: duckdb.DuckDBPyConnection,
    latitude: float,
    longitude: float,
    radius_km: float,
    month: int,
) -> List[Dict[str, Any]]:
    """
    API function to get popular hotspots within a radius for a given month.

    Args:
        latitude: Center latitude
        longitude: Center longitude
        radius_km: Search radius in kilometers
        month: Month number (1-12)

    Returns:
        List of hotspot dictionaries with locality info and average weekly checklists
    """
    hotspots = get_popular_hotspots(duck_db_conn, latitude, longitude, radius_km, month)

    return [hotspot.to_dict() for hotspot in hotspots]
