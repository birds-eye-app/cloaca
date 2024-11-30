from datetime import datetime
from cloaca.scripts.compute_patch_stats import compute_patch_stats


def test_compute_patch_stats():
    stats = compute_patch_stats()

    # make sure we only have 1 row per species
    species = set()
    for foy in stats["first_of_years"]:
        species.add(foy.speciesCode)
    assert len(species) == len(stats["first_of_years"])

    # find first obs of house sparrow
    house_sparrow = [
        foy for foy in stats["first_of_years"] if foy.speciesCode == "houspa"
    ][0]
    assert house_sparrow.obsDt == datetime(2024, 1, 1, 10, 38)

    # first crow was 2024-01-03 09:17
    crow = [foy for foy in stats["first_of_years"] if foy.speciesCode == "amecro"][0]
    assert crow.obsDt == datetime(2024, 1, 3, 9, 17)
    assert crow.firstName == "Michael"
    assert crow.lastName == "Lombardo"

    foy_leaderboard = stats["first_of_year_leaderboard"]
    assert len(foy_leaderboard) > 1
    leader = foy_leaderboard[0]
    assert leader[0] == "Michael  Lombardo"
    assert leader[1] > 40
