"""The Big Days endpoints over a tiny synthetic pair of tables in the documented shape."""

import datetime as dt

import duckdb
import pytest
from fastapi.testclient import TestClient

from cloaca.big_days import BigDays


def make_tables(directory):
    con = duckdb.connect()
    rows = []
    # Kings County: a 3-checklist solo day, a shared-party day, and a winter day.
    rows += [
        (
            "county",
            "US-NY-047",
            dt.date(2024, 5, 11),
            2024,
            5,
            "obsr1",
            120,
            3,
            3,
            1,
            True,
            1,
            ["obsr1"],
            600.0,
            12.0,
            True,
        ),
        (
            "county",
            "US-NY-047",
            dt.date(2023, 5, 13),
            2023,
            5,
            "obsr2",
            118,
            2,
            2,
            3,
            False,
            2,
            ["obsr2", "obsr3"],
            500.0,
            8.0,
            True,
        ),
        (
            "county",
            "US-NY-047",
            dt.date(2024, 1, 6),
            2024,
            1,
            "obsr1",
            61,
            1,
            1,
            1,
            True,
            1,
            ["obsr1"],
            90.0,
            2.0,
            True,
        ),
        (  # an aggregator account: 155 h of lists in one "day" — must never rank
            "county",
            "US-NY-047",
            dt.date(2021, 5, 8),
            2021,
            5,
            "obsr9",
            202,
            371,
            200,
            6,
            False,
            1,
            ["obsr9"],
            9353.0,
            270.0,
            True,
        ),
        (
            "state",
            "US-NY",
            dt.date(2024, 5, 11),
            2024,
            5,
            "obsr1",
            140,
            4,
            4,
            1,
            True,
            1,
            ["obsr1"],
            700.0,
            30.0,
            True,
        ),
        (
            "country",
            "US",
            dt.date(2024, 5, 11),
            2024,
            5,
            "obsr1",
            140,
            4,
            4,
            1,
            True,
            1,
            ["obsr1"],
            700.0,
            30.0,
            True,
        ),
    ]
    con.execute(
        """CREATE TABLE d (level VARCHAR, region VARCHAR, observation_date DATE, year INTEGER,
           month BIGINT, observer_id VARCHAR, n_species BIGINT, n_checklists BIGINT, n_localities BIGINT,
           observers SMALLINT, solo BOOLEAN, party_size BIGINT, members VARCHAR[], minutes DOUBLE,
           km DOUBLE, all_complete BOOLEAN)"""
    )
    con.executemany("INSERT INTO d VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute(
        f"""COPY (
              SELECT *, [{{'id': 'S1', 'locality': 'Prospect Park', 'locality_id': 'L109516', 'hotspot': true,
                           'lat': 40.66, 'lon': -73.97, 'time': '06:00:00', 'minutes': 300, 'km': 5.0,
                           'n_species': 90, 'complete': true, 'protocol': 'Traveling'}}] AS checklists
              FROM d ORDER BY level, region, n_species DESC
            ) TO '{directory}/big_days.parquet' (FORMAT parquet)"""
    )
    con.execute(
        f"""COPY (
              SELECT * FROM (VALUES
                ('country', 'US', 'United States', NULL, 'US', 5, 20, 140, DATE '2024-05-11', 2023, 2024, 40.0, -74.0,
                 [{{'year': 2024, 'days': 3, 'observers': 2, 'best': 140, 'best_date': DATE '2024-05-11'}}]),
                ('state', 'US-NY', 'New York', 'US', 'US', 5, 20, 140, DATE '2024-05-11', 2023, 2024, 40.7, -74.0,
                 [{{'year': 2024, 'days': 3, 'observers': 2, 'best': 140, 'best_date': DATE '2024-05-11'}}]),
                ('county', 'US-NY-047', 'Kings', 'US-NY', 'US', 3, 6, 202, DATE '2021-05-08', 2021, 2024, 40.66, -73.97,
                 [{{'year': 2023, 'days': 1, 'observers': 2, 'best': 118, 'best_date': DATE '2023-05-13'}},
                  {{'year': 2024, 'days': 2, 'observers': 1, 'best': 120, 'best_date': DATE '2024-05-11'}}])
              ) t(level, region, name, parent, country_code, days, checklists, best, best_date,
                  first_year, last_year, lat, lon, years)
            ) TO '{directory}/big_day_regions.parquet' (FORMAT parquet)"""
    )


@pytest.fixture()
def client(tmp_path, monkeypatch):
    make_tables(str(tmp_path))
    monkeypatch.setenv("BIG_DAYS_DIR", str(tmp_path))
    import cloaca.main as main

    fresh = BigDays(str(tmp_path))
    monkeypatch.setattr(main, "big_days", fresh)
    fresh.reload_if_changed()
    return TestClient(main.Cloaca_App)


def test_countries_and_tree(client):
    countries = client.get("/v1/big_days/regions").json()["regions"]
    assert [c["code"] for c in countries] == ["US"]
    r = client.get("/v1/big_days/regions/US-NY-047").json()
    assert [c["code"] for c in r["breadcrumb"]] == ["US", "US-NY", "US-NY-047"]
    assert r["region"]["name"] == "Kings" and "years" not in r["region"]
    # the 155-hour aggregator day is not the record, even though the regions file said so
    assert (r["region"]["best"], r["region"]["best_date"]) == (120, "2024-05-11")
    assert all(y["year"] != 2021 for y in r["years"])
    assert [(y["year"], y["best"], y["days"]) for y in r["years"]] == [
        (2023, 118, 1),
        (2024, 120, 2),
    ]
    assert (
        client.get("/v1/big_days/regions/US").json()["children"][0]["code"] == "US-NY"
    )


def test_top_filters_are_applied(client):
    top = client.get("/v1/big_days/top", params={"region": "US-NY-047"}).json()
    assert [r["n_species"] for r in top["rows"]] == [120, 118, 61]
    assert top["rows"][0]["rank"] == 1 and top["rows"][0]["checklists"][0]["id"] == "S1"
    solo = client.get(
        "/v1/big_days/top", params={"region": "US-NY-047", "solo": "true"}
    ).json()
    assert [r["observer_id"] for r in solo["rows"]] == ["obsr1", "obsr1"]
    jan = client.get(
        "/v1/big_days/top", params={"region": "US-NY-047", "month": 1}
    ).json()
    assert [r["n_species"] for r in jan["rows"]] == [61]
    y23 = client.get(
        "/v1/big_days/top", params={"region": "US-NY-047", "year": 2023, "month": 5}
    ).json()
    assert [r["party_size"] for r in y23["rows"]] == [2]
    assert y23["filters"] == {"year": 2023, "month": 5, "solo": False, "limit": 50}


def test_search_and_validation(client):
    hits = client.get("/v1/big_days/search", params={"q": "king"}).json()["results"]
    assert hits[0]["code"] == "US-NY-047" and [
        c["code"] for c in hits[0]["breadcrumb"]
    ] == ["US", "US-NY"]
    assert (
        client.get("/v1/big_days/search", params={"q": "' OR 1=1 --"}).status_code
        == 200
    )
    assert (
        client.get("/v1/big_days/top", params={"region": "US;DROP"}).status_code == 400
    )
    assert (
        client.get("/v1/big_days/top", params={"region": "US", "year": 99}).status_code
        == 400
    )
    assert client.get("/v1/big_days/regions/ZZ-ZZ").status_code == 404
    assert client.get("/v1/big_days/meta").json()["ready"] is True


def test_missing_files_are_503(tmp_path, monkeypatch):
    import cloaca.main as main

    monkeypatch.setattr(main, "big_days", BigDays(str(tmp_path / "nowhere")))
    c = TestClient(main.Cloaca_App)
    assert c.get("/v1/big_days/meta").json()["ready"] is False
    assert c.get("/v1/big_days/regions").status_code == 503
    assert c.get("/v1/big_days/top", params={"region": "US"}).status_code == 503
