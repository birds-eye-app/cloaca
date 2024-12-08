from cloaca.types import get_lifers_from_cache, group_lifers_by_location


async def get_lifers_by_location(latitude: float, longitude: float, file_id: str):
    lifers_from_csv = get_lifers_from_cache(file_id)

    lifers_by_location = group_lifers_by_location(lifers_from_csv)

    return lifers_by_location
