from cloaca.parsing.parse_ebird_regional_list import parse_subnational1_file


def test_parse_subnational1_file():
    sub_regions = parse_subnational1_file()

    assert len(sub_regions) == 3937

    # first line:
    # AC,Ashmore and Cartier Islands,AC-,Ashmore and Cartier Islands,
    first_region = sub_regions[0]
    assert first_region.country_code == "AC"
    assert first_region.country_name == "Ashmore and Cartier Islands"
    assert first_region.subnational1_code == "AC-"
    assert first_region.subnational1_name == "Ashmore and Cartier Islands"

    # find ny
    ny = [r for r in sub_regions if r.subnational1_code == "US-NY"][0]
    assert ny.country_code == "US"
    assert ny.country_name == "United States"
    assert ny.subnational1_code == "US-NY"
    assert ny.subnational1_name == "New York"
