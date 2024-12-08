from cloaca.scripts.fetch_yearly_hotspot_data import (
    eBirdHistoricFullObservation,
    parse_historic_observation_csv,
)


def first_of_years(
    observations: list[eBirdHistoricFullObservation],
) -> list[eBirdHistoricFullObservation]:
    # first of year will be the first time in the year a species was observed. We want:
    # - the name of the species
    # - the time of the first observation
    # - the observer
    first_of_years = []

    # sort by date
    observations.sort(key=lambda x: x.obsDt)

    # group by species
    species_groups = {}
    for obs in observations:
        # if we haven't seen this species yet, add it to the list (it's the FOY!)
        if obs.speciesCode not in species_groups:
            species_groups[obs.speciesCode] = []
            species_groups[obs.speciesCode].append(obs)
            first_of_years.append(obs)

    # print out the first of years to a csv
    with open("first_of_years.csv", "w") as f:
        f.write("speciesCode,comName,sciName,obsDt,userDisplayName\n")
        for foy in first_of_years:
            f.write(
                f"{foy.speciesCode},{foy.comName},{foy.sciName},{foy.obsDt},{foy.userDisplayName}\n"
            )

    return first_of_years


def first_of_year_leaderboard(
    foys: list[eBirdHistoricFullObservation],
) -> list[tuple[str, int]]:
    # group by observer
    observer_groups: dict[str, list[eBirdHistoricFullObservation]] = {}
    for foy in foys:
        if foy.userDisplayName not in observer_groups:
            observer_groups[foy.userDisplayName] = []
        observer_groups[foy.userDisplayName].append(foy)

    # sort by number of first of years
    leaderboard_full = sorted(
        observer_groups.items(),
        key=lambda x: len(x[1]),
        reverse=True,
    )
    # only keep length
    leaderboard = [(observer, len(foys)) for observer, foys in leaderboard_full]

    # print out the leaderboard to a csv
    with open("first_of_year_leaderboard.csv", "w") as f:
        f.write("userDisplayName,first_of_years\n")
        for observer, foy_count in leaderboard:
            f.write(f"{observer},{foy_count}\n")

    return leaderboard


def compute_patch_stats():
    observations = parse_historic_observation_csv()

    foy = first_of_years(observations)

    return {
        "first_of_years": foy,
        "first_of_year_leaderboard": first_of_year_leaderboard(foy),
    }
