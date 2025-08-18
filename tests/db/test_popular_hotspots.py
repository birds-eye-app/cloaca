import pytest
from unittest.mock import patch, MagicMock
from cloaca.db.popular_hotspots import (
    get_popular_hotspots,
    PopularHotspotResult,
    haversine_distance_km,
)


class TestHaversineDistance:
    def test_same_point(self):
        """Test distance between same point is 0."""
        distance = haversine_distance_km(43.6532, -79.3832, 43.6532, -79.3832)
        assert abs(distance) < 0.001

    def test_known_distance(self):
        """Test known distance between Toronto and Ottawa (approximately 350km)."""
        toronto_lat, toronto_lon = 43.6532, -79.3832
        ottawa_lat, ottawa_lon = 45.4215, -75.6972

        distance = haversine_distance_km(
            toronto_lat, toronto_lon, ottawa_lat, ottawa_lon
        )
        # Should be approximately 350km, allow 10km tolerance
        assert 340 <= distance <= 360

    def test_short_distance(self):
        """Test short distance calculation."""
        # Points about 10km apart in Toronto
        lat1, lon1 = 43.6532, -79.3832
        lat2, lon2 = 43.7532, -79.3832  # About 11km north

        distance = haversine_distance_km(lat1, lon1, lat2, lon2)
        assert 10 <= distance <= 12


class TestPopularHotspotResult:
    def test_create_hotspot_result(self):
        """Test creating a PopularHotspotResult object."""
        hotspot = PopularHotspotResult(
            locality_id="L123",
            locality_name="Test Park",
            locality_type="H",
            latitude=43.6532,
            longitude=-79.3832,
            avg_weekly_checklists=15.5,
            country_code="CA",
            state_code="CA-ON",
            county_code="CA-ON-TO",
        )

        assert hotspot.locality_id == "L123"
        assert hotspot.locality_name == "Test Park"
        assert hotspot.avg_weekly_checklists == 15.5

    def test_to_dict(self):
        """Test converting hotspot to dictionary."""
        hotspot = PopularHotspotResult(
            locality_id="L123",
            locality_name="Test Park",
            locality_type="H",
            latitude=43.6532,
            longitude=-79.3832,
            avg_weekly_checklists=15.5,
            country_code="CA",
            state_code="CA-ON",
            county_code="CA-ON-TO",
        )

        result = hotspot.to_dict()
        expected = {
            "locality_id": "L123",
            "locality_name": "Test Park",
            "locality_type": "H",
            "latitude": 43.6532,
            "longitude": -79.3832,
            "avg_weekly_checklists": 15.5,
            "country_code": "CA",
            "state_code": "CA-ON",
            "county_code": "CA-ON-TO",
        }

        assert result == expected


class TestGetPopularHotspots:
    @patch("cloaca.db.popular_hotspots.get_db_connection")
    def test_get_popular_hotspots_basic(self, mock_get_db_connection):
        """Test basic functionality of get_popular_hotspots."""
        # Mock database connection and results
        mock_con = MagicMock()
        mock_get_db_connection.return_value = mock_con

        # Mock query results (locality_id, name, type, lat, lon, avg_checklists, country, state, county)
        mock_results = [
            (
                "L123",
                "Close Park",
                "H",
                43.6532,
                -79.3832,
                15.5,
                "CA",
                "CA-ON",
                "CA-ON-TO",
            ),
            (
                "L456",
                "Far Park",
                "H",
                44.6532,
                -80.3832,
                10.2,
                "CA",
                "CA-ON",
                "CA-ON-TO",
            ),  # Far away
            (
                "L789",
                "Another Close Park",
                "H",
                43.6632,
                -79.3732,
                8.8,
                "CA",
                "CA-ON",
                "CA-ON-TO",
            ),
        ]
        mock_con.execute.return_value.fetchall.return_value = mock_results

        # Call the function
        result = get_popular_hotspots(43.6532, -79.3832, 50.0, 10)

        # Verify database connection and query
        mock_get_db_connection.assert_called_once()
        mock_con.execute.assert_called_once()
        mock_con.close.assert_called_once()

        # Verify results - should filter out the far park and sort by avg_checklists
        assert len(result) == 2
        assert result[0].locality_id == "L123"  # Higher avg_checklists
        assert result[0].avg_weekly_checklists == 15.5
        assert result[1].locality_id == "L789"  # Lower avg_checklists
        assert result[1].avg_weekly_checklists == 8.8

    @patch("cloaca.db.popular_hotspots.get_db_connection")
    def test_get_popular_hotspots_empty_results(self, mock_get_db_connection):
        """Test handling of empty database results."""
        mock_con = MagicMock()
        mock_get_db_connection.return_value = mock_con
        mock_con.execute.return_value.fetchall.return_value = []

        result = get_popular_hotspots(43.6532, -79.3832, 50.0, 10)

        assert result == []
        mock_con.close.assert_called_once()

    @patch("cloaca.db.popular_hotspots.get_db_connection")
    def test_get_popular_hotspots_distance_filtering(self, mock_get_db_connection):
        """Test that distance filtering works correctly."""
        mock_con = MagicMock()
        mock_get_db_connection.return_value = mock_con

        # Mock results with one close and one far location
        mock_results = [
            (
                "L123",
                "Close Park",
                "H",
                43.6532,
                -79.3832,
                15.5,
                "CA",
                "CA-ON",
                "CA-ON-TO",
            ),  # Same location
            (
                "L456",
                "Far Park",
                "H",
                45.0,
                -75.0,
                10.2,
                "CA",
                "CA-ON",
                "CA-ON-TO",
            ),  # ~400km away
        ]
        mock_con.execute.return_value.fetchall.return_value = mock_results

        # Test with small radius
        result = get_popular_hotspots(43.6532, -79.3832, 10.0, 10)

        # Should only return the close park
        assert len(result) == 1
        assert result[0].locality_id == "L123"

    @patch("cloaca.db.popular_hotspots.get_db_connection")
    def test_database_connection_closed_on_exception(self, mock_get_db_connection):
        """Test that database connection is properly closed even if exception occurs."""
        mock_con = MagicMock()
        mock_get_db_connection.return_value = mock_con
        mock_con.execute.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            get_popular_hotspots(43.6532, -79.3832, 50.0, 10)

        # Verify connection was still closed
        mock_con.close.assert_called_once()
