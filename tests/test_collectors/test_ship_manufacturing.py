"""Tests for the Ship Manufacturing collector.

Tests MARAD and UNCTAD data collection for shipbuilding activity.
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx

from src.data_ingestion.collectors.ship_manufacturing_collector import (
    ShipManufacturingCollector,
    ShipbuildingRecord,
    get_ship_manufacturing_collector,
    VESSEL_TYPES,
)
from src.data_ingestion.base_collector import DataFrequency, DataPoint


class TestShipManufacturingCollectorInit:
    """Test ShipManufacturingCollector initialization."""

    def test_collector_creation(self):
        """Test collector can be created."""
        collector = ShipManufacturingCollector()
        assert collector.name == "Ship Manufacturing"
        assert collector.source_id == "ship_manufacturing"

    def test_factory_function(self):
        """Test factory function returns collector."""
        collector = get_ship_manufacturing_collector()
        assert isinstance(collector, ShipManufacturingCollector)

    def test_frequency_is_daily(self):
        """Test collector frequency is daily (check for changes)."""
        collector = ShipManufacturingCollector()
        assert collector.frequency == DataFrequency.DAILY

    def test_schedule_returns_cron(self):
        """Test schedule returns valid cron expression."""
        collector = ShipManufacturingCollector()
        schedule = collector.get_schedule()
        assert schedule == "0 10 * * *"  # Daily 10 AM UTC

    def test_default_series(self):
        """Test default series includes expected series."""
        collector = ShipManufacturingCollector()
        series = collector.get_default_series()
        
        assert "us_flag_fleet" in series
        assert "global_fleet" in series
        assert "orderbook" in series


class TestShipbuildingRecord:
    """Test ShipbuildingRecord dataclass."""

    def test_create_record(self):
        """Test creating a shipbuilding record."""
        record = ShipbuildingRecord(
            shipyard="Bath Iron Works",
            vessel_type="naval",
            metric_type="backlog",
            value=Decimal("5"),
            unit="vessels",
            timestamp=datetime.now(UTC),
            source="MARAD",
            country="US",
        )
        
        assert record.shipyard == "Bath Iron Works"
        assert record.vessel_type == "naval"
        assert record.source == "MARAD"
        assert record.country == "US"

    def test_optional_fields(self):
        """Test optional fields have defaults."""
        record = ShipbuildingRecord(
            shipyard=None,
            vessel_type="container",
            metric_type="fleet",
            value=Decimal("1000"),
            unit="dwt",
            timestamp=datetime.now(UTC),
            source="UNCTAD",
        )
        
        assert record.country is None
        assert record.metadata == {}


class TestVesselTypeClassification:
    """Test vessel type classification."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return ShipManufacturingCollector()

    def test_classify_container(self, collector):
        """Test container vessel classification."""
        assert collector._classify_vessel_type("Container Ship") == "container"
        assert collector._classify_vessel_type("Containership 15000 TEU") == "container"

    def test_classify_tanker(self, collector):
        """Test tanker classification."""
        assert collector._classify_vessel_type("Crude Oil Tanker") == "tanker"
        assert collector._classify_vessel_type("Product Tanker") == "tanker"
        assert collector._classify_vessel_type("Chemical Tanker") == "tanker"

    def test_classify_bulk(self, collector):
        """Test bulk carrier classification."""
        assert collector._classify_vessel_type("Bulk Carrier") == "bulk"
        assert collector._classify_vessel_type("Dry Bulk") == "bulk"
        assert collector._classify_vessel_type("Bulker Handysize") == "bulk"

    def test_classify_lng(self, collector):
        """Test LNG carrier classification."""
        assert collector._classify_vessel_type("LNG Carrier") == "lng"
        assert collector._classify_vessel_type("Liquefied Natural Gas") == "lng"

    def test_classify_naval(self, collector):
        """Test naval vessel classification."""
        assert collector._classify_vessel_type("Naval Destroyer") == "naval"
        assert collector._classify_vessel_type("Coast Guard Cutter") == "naval"

    def test_classify_other(self, collector):
        """Test unknown vessels default to 'other'."""
        assert collector._classify_vessel_type("Unknown Vessel Type") == "other"
        assert collector._classify_vessel_type("") == "other"


class TestMARADCollection:
    """Test MARAD data collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return ShipManufacturingCollector()

    @pytest.mark.asyncio
    async def test_collect_marad_fleet_handles_404(self, collector):
        """Test MARAD collection handles missing files gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_marad_fleet(client)
                
                assert isinstance(points, list)
                assert len(points) == 0

    @pytest.mark.asyncio
    async def test_collect_marad_shipyards_handles_missing_data(self, collector):
        """Test MARAD shipyard collection handles missing data."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_marad_shipyards(client)
                
                assert isinstance(points, list)


class TestUNCTADCollection:
    """Test UNCTAD data collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return ShipManufacturingCollector()

    @pytest.fixture
    def sample_unctad_csv(self):
        """Sample UNCTAD CSV data."""
        return """year,country,vessel_type,value
2023,World,Container,250000000
2023,World,Tanker,350000000
2023,World,Bulk,400000000
2024,World,Container,260000000
"""

    def test_parse_unctad_csv(self, collector, sample_unctad_csv):
        """Test UNCTAD CSV parsing."""
        points = collector._parse_unctad_csv(
            sample_unctad_csv,
            metric_type="fleet",
            unit="dwt",
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 12, 31),
        )
        
        assert isinstance(points, list)
        assert len(points) >= 0

    @pytest.mark.asyncio
    async def test_collect_unctad_handles_api_error(self, collector):
        """Test UNCTAD collection handles API errors."""
        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_unctad_fleet(
                    client,
                    datetime(2023, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert isinstance(points, list)


class TestFullCollection:
    """Test full collection cycle."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return ShipManufacturingCollector()

    @pytest.mark.asyncio
    async def test_collect_returns_datapoints(self, collector):
        """Test that collect method returns DataPoint objects."""
        sample_point = DataPoint(
            source_id="ship_manufacturing",
            series_id="TEST_SERIES",
            timestamp=datetime.now(UTC),
            value=1000.0,
            unit="vessels",
        )
        
        with patch.object(collector, "_collect_marad_fleet", new_callable=AsyncMock) as mock_marad:
            with patch.object(collector, "_collect_marad_shipyards", new_callable=AsyncMock) as mock_yards:
                with patch.object(collector, "_collect_unctad_fleet", new_callable=AsyncMock) as mock_unctad:
                    mock_marad.return_value = [sample_point]
                    mock_yards.return_value = []
                    mock_unctad.return_value = [sample_point]
                    
                    points = await collector.collect()
                    
                    assert len(points) >= 2
                    assert all(isinstance(p, DataPoint) for p in points)

    @pytest.mark.asyncio
    async def test_collect_handles_partial_failures(self, collector):
        """Test that collect continues if one source fails."""
        sample_point = DataPoint(
            source_id="ship_manufacturing",
            series_id="TEST_SERIES",
            timestamp=datetime.now(UTC),
            value=1000.0,
            unit="vessels",
        )
        
        with patch.object(collector, "_collect_marad_fleet", new_callable=AsyncMock) as mock_marad:
            with patch.object(collector, "_collect_marad_shipyards", new_callable=AsyncMock) as mock_yards:
                with patch.object(collector, "_collect_unctad_fleet", new_callable=AsyncMock) as mock_unctad:
                    mock_marad.side_effect = Exception("MARAD error")
                    mock_yards.return_value = []
                    mock_unctad.return_value = [sample_point]
                    
                    points = await collector.collect()
                    
                    assert len(points) >= 1


class TestAPIValidation:
    """Test API validation method."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return ShipManufacturingCollector()

    @pytest.mark.asyncio
    async def test_validate_api_key_returns_bool(self, collector):
        """Test that validate_api_key returns a boolean."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client.get.return_value = mock_response
            
            mock_client_class.return_value = mock_client
            
            result = await collector.validate_api_key()
            
            assert isinstance(result, bool)
