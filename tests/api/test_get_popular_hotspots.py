import pytest
from unittest.mock import patch
from cloaca.api.get_popular_hotspots import get_popular_hotspots_api
from cloaca.db.popular_hotspots import PopularHotspotResult


class TestGetPopularHotspotsApi:
    @patch("cloaca.api.get_popular_hotspots.get_popular_hotspots")
    @pytest.mark.asyncio
    async def test_get_popular_hotspots_api_success(self, mock_get_popular_hotspots):
        """Test successful API call returns formatted results."""
        # Mock the database function to return sample hotspots
        mock_hotspots = [
            PopularHotspotResult(
                locality_id="L123",
                locality_name="High Park",
                latitude=43.6446,
                longitude=-79.4625,
                avg_weekly_checklists=15.5,
            ),
            PopularHotspotResult(
                locality_id="L456",
                locality_name="Tommy Thompson Park",
                latitude=43.6283,
                longitude=-79.3292,
                avg_weekly_checklists=12.3,
            ),
        ]
        mock_get_popular_hotspots.return_value = mock_hotspots

        # Call the API function
        result = await get_popular_hotspots_api(43.6532, -79.3832, 50.0, 10)

        # Verify the database function was called with correct parameters
        mock_get_popular_hotspots.assert_called_once_with(43.6532, -79.3832, 50.0, 10)

        # Verify the result format
        assert len(result) == 2
        assert result[0] == {
            "locality_id": "L123",
            "locality_name": "High Park",
            "latitude": 43.6446,
            "longitude": -79.4625,
            "avg_weekly_checklists": 15.5,
        }
        assert result[1]["locality_id"] == "L456"
        assert result[1]["locality_name"] == "Tommy Thompson Park"

    @patch("cloaca.api.get_popular_hotspots.get_popular_hotspots")
    @pytest.mark.asyncio
    async def test_get_popular_hotspots_api_empty_results(
        self, mock_get_popular_hotspots
    ):
        """Test API call with no results."""
        mock_get_popular_hotspots.return_value = []

        result = await get_popular_hotspots_api(43.6532, -79.3832, 50.0, 10)

        assert result == []
        mock_get_popular_hotspots.assert_called_once_with(43.6532, -79.3832, 50.0, 10)

    @patch("cloaca.api.get_popular_hotspots.get_popular_hotspots")
    @pytest.mark.asyncio
    async def test_get_popular_hotspots_api_propagates_exception(
        self, mock_get_popular_hotspots
    ):
        """Test that API function propagates exceptions from database layer."""
        mock_get_popular_hotspots.side_effect = Exception("Database connection failed")

        with pytest.raises(Exception, match="Database connection failed"):
            await get_popular_hotspots_api(43.6532, -79.3832, 50.0, 10)
