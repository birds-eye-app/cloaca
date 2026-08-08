import datetime

from cloaca.achievements.config import Patch, Region
from cloaca.achievements.engine import (
    AchievementType,
    Tier,
    compute_achievements,
    filter_since,
    summarize,
)
from cloaca.parsing.parsing_helpers import Observation

MCGOLRICK = Patch(
    name="McGolrick Park",
    location_ids=frozenset({"L2987624"}),
    name_substrings=("mcgolrick",),
)
HOUSE = Patch(name="My House", location_ids=frozenset({"L23333013"}))
PATCHES = (MCGOLRICK, HOUSE)

NY = Region(name="New York", state_codes=frozenset({"US-NY"}))
VINEYARD = Region(name="Martha's Vineyard", counties=frozenset({("US-MA", "Dukes")}))
REGIONS = (NY, VINEYARD)


def mk_obs(
    common_name: str,
    scientific_name: str,
    date: str,
    submission_id: str = "S1",
    location_id: str = "L1",
    location: str = "Some Park",
    state_province: str = "US-NY",
    county: str = "Kings",
    time: str = "09:00 AM",
    taxonomic_order: int = 1,
) -> Observation:
    return Observation(
        submission_id=submission_id,
        common_name=common_name,
        scientific_name=scientific_name,
        taxonomic_order=taxonomic_order,
        count=1,
        state_province=state_province,
        county=county,
        location_id=location_id,
        location=location,
        latitude=40.7,
        longitude=-73.9,
        date=date,
        time=time,
        protocol="eBird - Traveling Count",
        duration_min=60,
        all_obs_reported="1",
        distance_traveled_km=1.0,
        area_covered_ha=0.0,
        number_of_observers=1,
        breeding_code="",
        observation_details="",
        checklist_comments="",
        ml_catalog_numbers="",
    )


def events_of(achievements, type_):
    return [a for a in achievements if a.type == type_]


def compute(observations):
    return compute_achievements(observations, patches=PATCHES, regions=REGIONS)


class TestLifers:
    def test_first_sighting_is_lifer_second_is_not(self):
        obs = [
            mk_obs("Snow Goose", "Anser caerulescens", "2024-01-01", "S1"),
            mk_obs("Snow Goose", "Anser caerulescens", "2024-01-02", "S2"),
        ]
        lifers = events_of(compute(obs), AchievementType.LIFER)
        assert len(lifers) == 1
        assert lifers[0].date == "2024-01-01"
        assert lifers[0].context["number"] == 1

    def test_lifer_milestone_is_gold(self):
        obs = [
            mk_obs(f"Bird {i}", f"Species number{i}", "2024-01-01", "S1")
            for i in range(25)
        ]
        lifers = events_of(compute(obs), AchievementType.LIFER)
        assert len(lifers) == 25
        assert lifers[24].context["milestone"] is True
        assert lifers[24].tier == Tier.GOLD
        assert lifers[23].tier == Tier.SILVER

    def test_spuhs_and_hybrids_are_ignored(self):
        obs = [
            mk_obs("gull sp.", "Larus sp.", "2024-01-01"),
            mk_obs("Mallard x American Black Duck", "Anas platyrhynchos x rubripes", "2024-01-01"),
        ]
        assert events_of(compute(obs), AchievementType.LIFER) == []


class TestPatchFirsts:
    def test_patch_first_by_location_id(self):
        obs = [
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-01", "S1", "L9", "Central Park"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-02", "S2", "L2987624", "McGolrick Park"),
        ]
        firsts = events_of(compute(obs), AchievementType.PATCH_FIRST)
        assert len(firsts) == 1
        assert firsts[0].date == "2024-05-02"
        assert firsts[0].context["patch"] == "McGolrick Park"
        assert firsts[0].tier == Tier.GOLD

    def test_patch_first_by_name_substring(self):
        obs = [
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-02", "S1", "L999", "McGolrick Park--north end"),
        ]
        firsts = events_of(compute(obs), AchievementType.PATCH_FIRST)
        assert len(firsts) == 1

    def test_house_patch_and_patch_foy(self):
        obs = [
            mk_obs("House Finch", "Haemorhous mexicanus", "2024-05-01", "S1", "L23333013", "26 King"),
            mk_obs("House Finch", "Haemorhous mexicanus", "2024-06-01", "S2", "L23333013", "26 King"),
            mk_obs("House Finch", "Haemorhous mexicanus", "2025-03-01", "S3", "L23333013", "26 King"),
        ]
        result = compute(obs)
        firsts = events_of(result, AchievementType.PATCH_FIRST)
        foys = events_of(result, AchievementType.PATCH_FIRST_OF_YEAR)
        assert len(firsts) == 1
        assert firsts[0].context["patch"] == "My House"
        # repeat within 2024 is not a patch FOY; 2025 sighting is
        assert len(foys) == 1
        assert foys[0].date == "2025-03-01"


class TestRegionFirsts:
    def test_state_region_first(self):
        obs = [
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-01", "S1", state_province="US-MA", county="Suffolk"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-02", "S2", state_province="US-NY"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-03", "S3", state_province="US-NY"),
        ]
        firsts = events_of(compute(obs), AchievementType.REGION_FIRST)
        assert len(firsts) == 1
        assert firsts[0].date == "2024-05-02"
        assert firsts[0].context["region"] == "New York"

    def test_county_region_first(self):
        obs = [
            mk_obs("Osprey", "Pandion haliaetus", "2024-07-01", "S1", state_province="US-MA", county="Suffolk"),
            mk_obs("Osprey", "Pandion haliaetus", "2024-07-02", "S2", state_province="US-MA", county="Dukes"),
        ]
        firsts = events_of(compute(obs), AchievementType.REGION_FIRST)
        assert len(firsts) == 1
        assert firsts[0].context["region"] == "Martha's Vineyard"


class TestFirstOfYear:
    def test_foy_not_emitted_for_lifer_year(self):
        obs = [
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-01", "S1"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-06-01", "S2"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2025-05-01", "S3"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2025-06-01", "S4"),
        ]
        foys = events_of(compute(obs), AchievementType.FIRST_OF_YEAR)
        assert len(foys) == 1
        assert foys[0].date == "2025-05-01"


class TestReunion:
    def test_reunion_after_a_year(self):
        obs = [
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2020-05-01", "S1"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-01", "S2"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-02", "S3"),
        ]
        reunions = events_of(compute(obs), AchievementType.REUNION)
        assert len(reunions) == 1
        assert reunions[0].date == "2024-05-01"
        assert reunions[0].context["gap_days"] == (
            datetime.date(2024, 5, 1) - datetime.date(2020, 5, 1)
        ).days
        # 4-year gap counts as long-lost (silver)
        assert reunions[0].tier == Tier.SILVER

    def test_no_reunion_within_a_year(self):
        obs = [
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-05-01", "S1"),
            mk_obs("Ovenbird", "Seiurus aurocapilla", "2024-11-01", "S2"),
        ]
        assert events_of(compute(obs), AchievementType.REUNION) == []


class TestChecklistEvents:
    def test_spark_day_and_dawn_patrol_and_night_owl(self):
        obs = [
            mk_obs("Rock Pigeon", "Columba livia", "2024-01-01", "S1", time="05:30 AM"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-01-02", "S2", time="10:00 PM"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-01-03", "S3", time="05:00 AM"),
        ]
        result = compute(obs)
        sparks = events_of(result, AchievementType.SPARK_DAY)
        assert len(sparks) == 1
        assert sparks[0].date == "2024-01-01"
        # dawn patrol and night owl are once-ever badges
        assert len(events_of(result, AchievementType.DAWN_PATROL)) == 1
        assert len(events_of(result, AchievementType.NIGHT_OWL)) == 1

    def test_big_checklist(self):
        obs = [
            mk_obs(f"Bird {i}", f"Species number{i}", "2024-05-10", "S1")
            for i in range(52)
        ]
        big = events_of(compute(obs), AchievementType.BIG_CHECKLIST)
        assert len(big) == 1
        assert big[0].context["species_count"] == 52
        assert big[0].tier == Tier.SILVER

    def test_new_state_and_country(self):
        obs = [
            mk_obs("Rock Pigeon", "Columba livia", "2024-01-01", "S1", state_province="US-NY"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-02-01", "S2", state_province="US-MA"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-03-01", "S3", state_province="CR-A"),
        ]
        result = compute(obs)
        states = events_of(result, AchievementType.NEW_STATE)
        countries = events_of(result, AchievementType.NEW_COUNTRY)
        # first state/country ever isn't an event
        assert [s.context["state"] for s in states] == ["US-MA", "CR-A"]
        assert [c.context["country"] for c in countries] == ["CR"]
        assert countries[0].tier == Tier.GOLD


class TestBigDaysAndStreaks:
    def test_big_day_record(self):
        obs = []
        # day 1: 20 species (baseline), day 2: 10 (no event), day 3: 25 (record!)
        for i in range(20):
            obs.append(mk_obs(f"Bird {i}", f"Species number{i}", "2024-05-01", "S1"))
        for i in range(10):
            obs.append(mk_obs(f"Bird {i}", f"Species number{i}", "2024-05-02", "S2"))
        for i in range(25):
            obs.append(mk_obs(f"Bird {i}", f"Species number{i}", "2024-05-03", "S3"))
        big_days = events_of(compute(obs), AchievementType.BIG_DAY)
        assert len(big_days) == 1
        assert big_days[0].date == "2024-05-03"
        assert big_days[0].context == {"count": 25, "previous_record": 20}

    def test_streak(self):
        obs = [
            mk_obs("Rock Pigeon", "Columba livia", f"2024-05-{day:02d}", f"S{day}")
            for day in range(1, 6)
        ]
        streaks = events_of(compute(obs), AchievementType.STREAK)
        assert [s.context["length"] for s in streaks] == [3, 5]

    def test_streak_broken_by_gap(self):
        obs = [
            mk_obs("Rock Pigeon", "Columba livia", "2024-05-01", "S1"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-05-02", "S2"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-05-04", "S3"),
            mk_obs("Rock Pigeon", "Columba livia", "2024-05-05", "S4"),
        ]
        assert events_of(compute(obs), AchievementType.STREAK) == []


class TestPatchVisits:
    def test_visit_milestone(self):
        obs = [
            mk_obs(
                "Rock Pigeon",
                "Columba livia",
                f"2024-{1 + n // 28:02d}-{1 + n % 28:02d}",
                f"S{n}",
                "L2987624",
                "McGolrick Park",
            )
            for n in range(10)
        ]
        visits = events_of(compute(obs), AchievementType.PATCH_VISIT_MILESTONE)
        assert len(visits) == 1
        assert visits[0].context == {"patch": "McGolrick Park", "visits": 10}


class TestFilterSinceAndSummary:
    def test_filter_since(self):
        obs = [
            mk_obs("Snow Goose", "Anser caerulescens", "2024-01-01", "S1"),
            mk_obs("Brant", "Branta bernicla", "2024-06-01", "S2"),
        ]
        result = compute(obs)
        recent = filter_since(result, datetime.date(2024, 3, 1))
        assert all(a.date >= "2024-06-01" for a in recent)
        assert any(a.type == AchievementType.LIFER and a.species == "Brant" for a in recent)
        assert not any(a.species == "Snow Goose" for a in recent)

    def test_summary(self):
        obs = [
            mk_obs("Snow Goose", "Anser caerulescens", "2024-01-01", "S1", "L2987624", "McGolrick Park"),
            mk_obs("Brant", "Branta bernicla", "2024-01-02", "S2", state_province="US-MA", county="Dukes"),
        ]
        result = compute(obs)
        summary = summarize(obs, result, patches=PATCHES, regions=REGIONS)
        assert summary.life_list_total == 2
        assert summary.total_checklists == 2
        assert summary.patch_totals["McGolrick Park"] == 1
        assert summary.patch_totals["My House"] == 0
        assert summary.region_totals["New York"] == 1
        assert summary.region_totals["Martha's Vineyard"] == 1
        assert summary.longest_streak == 2

    def test_deterministic_replay(self):
        obs = [
            mk_obs("Snow Goose", "Anser caerulescens", "2024-01-01", "S1"),
            mk_obs("Brant", "Branta bernicla", "2024-06-01", "S2"),
            mk_obs("Osprey", "Pandion haliaetus", "2024-07-01", "S3"),
        ]
        first = compute(obs)
        second = compute(list(reversed(obs)))
        assert [a.key for a in first] == [a.key for a in second]
