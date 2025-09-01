import pytest
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from cloaca.db.db import get_db_connection_with_path
from cloaca.db.popular_hotspots import get_popular_hotspots


class TestPopularHotspotsIntegration:
    """Test integration between build_parsed_db output and popular_hotspots functionality."""

    @pytest.fixture
    def test_input_db(self):
        """Path to the test input database with 100 rows."""
        return Path(__file__).parent.parent / "data" / "test_input.db"

    @pytest.fixture
    def test_output_parsed_db(self):
        """Path to the test output database."""
        return Path(__file__).parent.parent / "data" / "test_parsed.db"

    def test_popular_hotspots_function_with_test_data(self, test_output_parsed_db):
        """Test that popular_hotspots function works with our test database."""
        con = get_db_connection_with_path(
            test_output_parsed_db, read_only=True, verify_tables_exist=False
        )

        try:
            # Test the function with some reasonable parameters
            # Using NYC area coordinates as a starting point
            latitude = 33
            longitude = -87
            radius_km = 1000  # 50km radius
            month = 3

            # This should not raise an error even if no data is returned
            results = get_popular_hotspots(con, latitude, longitude, radius_km, month)

            # Results should be a list (may be empty with test data)
            assert isinstance(results, list)

            assert len(results) == 7, "Expected 7 results"

            first_result = results[0].to_dict()

            #   {'locality_name': 'River Bend Turf'} != {'locality_name': 'Prospect Park'}
            #   {'latitude': 33.1335213} != {'latitude': 40.6602841}
            #   {'locality_id': 'L5039140'} != {'locality_id': 'L109516'}
            #   {'avg_weekly_checklists': 3.8333333333333335} != {'avg_weekly_checklists': 36.0}
            #   {'longitude': -87.6534175} != {'longitude': -73.9689534}
            #   {'likely_common_species_std_error': 0.6923430096239916} != {'likely_common_species_std_error': 0.22566773346211003}
            #   {'likely_common_and_uncommon_species_count': 58} != {'likely_common_and_uncommon_species_count': 88}
            #   {'likely_uncommon_species_count': 0} != {'likely_uncommon_species_count': 22}
            #   {'likely_common_species_count': 58} != {'likely_common_species_count': 66}

            assert first_result == {
                "locality_id": "L5039140",
                "locality_name": "River Bend Turf",
                "latitude": 33.1335213,
                "longitude": -87.6534175,
                "likely_uncommon_species_count": 0,
                "avg_weekly_checklists": 3.8333333333333335,
                "likely_common_and_uncommon_species_count": 58,
                "likely_common_species_count": 58,
                "likely_common_species_std_error": 0.6923430096239916,
            }

        finally:
            con.close()
