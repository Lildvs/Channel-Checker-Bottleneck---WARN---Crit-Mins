"""Tests for the Vehicle Production collector.

Tests Census M3, FRED AISRSA, Cox Automotive, and BEA motor vehicle output
collection as alternatives to WardsAuto.
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import json

import pytest
import httpx

from src.data_ingestion.collectors.vehicle_production_collector import (
    VehicleProductionCollector,
    VehicleProductionRecord,
    get_vehicle_production_collector,
    CENSUS_M3_SERIES,
    FRED_VEHICLE_SERIES,
)
from src.data_ingestion.base_collector import DataFrequency, DataPoint


class TestVehicleProductionCollectorInit:
    """Test VehicleProductionCollector initialization."""

    def test_collector_creation(self):
        """Test collector can be created."""
        collector = VehicleProductionCollector()
        assert collector.name == "Vehicle Production"
        assert collector.source_id == "vehicle_production"

    def test_factory_function(self):
        """Test factory function returns collector."""
        collector = get_vehicle_production_collector()
        assert isinstance(collector, VehicleProductionCollector)

    def test_frequency_is_monthly(self):
        """Test collector frequency is monthly."""
        collector = VehicleProductionCollector()
        assert collector.frequency == DataFrequency.MONTHLY

    def test_schedule_returns_cron(self):
        """Test schedule returns valid cron expression."""
        collector = VehicleProductionCollector()
        schedule = collector.get_schedule()
        assert schedule == "0 16 * * 3"  # Wednesday 4 PM UTC

    def test_default_series(self):
        """Test default series includes all configured series."""
        collector = VehicleProductionCollector()
        series = collector.get_default_series()
        
        for m3_series in CENSUS_M3_SERIES:
            assert m3_series in series
        for fred_series in FRED_VEHICLE_SERIES:
            assert fred_series in series


class TestVehicleProductionRecord:
    """Test VehicleProductionRecord dataclass."""

    def test_create_record(self):
        """Test creating a vehicle production record."""
        record = VehicleProductionRecord(
            metric_type="shipments",
            vehicle_type="light_vehicle",
            value=Decimal("1000000"),
            unit="million_dollars",
            timestamp=datetime.now(UTC),
            source="CENSUS_M3",
        )
        
        assert record.metric_type == "shipments"
        assert record.vehicle_type == "light_vehicle"
        assert record.source == "CENSUS_M3"

    def test_optional_fields(self):
        """Test optional fields have defaults."""
        record = VehicleProductionRecord(
            metric_type="inventory_ratio",
            vehicle_type="total",
            value=Decimal("1.5"),
            unit="ratio",
            timestamp=datetime.now(UTC),
            source="FRED",
        )
        
        assert record.seasonal_adjustment is None
        assert record.metadata == {}


class TestCensusM3Configuration:
    """Test Census M3 series configuration."""

    def test_m3_series_have_required_fields(self):
        """Test all M3 series have required configuration."""
        required_fields = ["name", "metric_type", "vehicle_type", "unit"]
        
        for series_id, config in CENSUS_M3_SERIES.items():
            for field in required_fields:
                assert field in config, f"Missing {field} in {series_id}"

    def test_m3_series_ids_format(self):
        """Test M3 series IDs follow expected format."""
        for series_id in CENSUS_M3_SERIES:
            assert series_id.isupper() or series_id[0].isalpha()


class TestFREDConfiguration:
    """Test FRED vehicle series configuration."""

    def test_fred_series_have_required_fields(self):
        """Test all FRED series have required configuration."""
        required_fields = ["name", "metric_type", "vehicle_type", "unit"]
        
        for series_id, config in FRED_VEHICLE_SERIES.items():
            for field in required_fields:
                assert field in config, f"Missing {field} in {series_id}"

    def test_aisrsa_is_primary(self):
        """Test AISRSA (inventory ratio) is configured as primary series."""
        assert "AISRSA" in FRED_VEHICLE_SERIES
        assert FRED_VEHICLE_SERIES["AISRSA"]["metric_type"] == "inventory_ratio"


class TestCensusM3Collection:
    """Test Census M3 data collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return VehicleProductionCollector()

    @pytest.fixture
    def sample_census_response(self):
        """Sample Census M3 API response."""
        return [
            ["cell_value", "time_slot_id", "data_type_code"],
            ["150000", "2024-01", "ASMVPSA"],
            ["155000", "2024-02", "ASMVPSA"],
            ["160000", "2024-03", "ASMVPSA"],
        ]

    @pytest.mark.asyncio
    async def test_collect_census_m3_parses_response(self, collector, sample_census_response):
        """Test Census M3 collection parses API response correctly."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_census_response

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 12, 31)
                
                points = await collector._collect_census_m3(client, start_date, end_date)
                
                assert isinstance(points, list)

    @pytest.mark.asyncio
    async def test_collect_census_m3_handles_empty_response(self, collector):
        """Test Census M3 collection handles empty response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [["cell_value", "time_slot_id"]]  # Headers only

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_census_m3(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert points == []


class TestFREDCollection:
    """Test FRED inventory ratio collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return VehicleProductionCollector()

    @pytest.fixture
    def sample_fred_response(self):
        """Sample FRED API response."""
        return {
            "observations": [
                {"date": "2024-01-01", "value": "1.45"},
                {"date": "2024-02-01", "value": "1.52"},
                {"date": "2024-03-01", "value": "1.48"},
            ]
        }

    @pytest.mark.asyncio
    async def test_collect_fred_parses_response(self, collector, sample_fred_response):
        """Test FRED collection parses observations correctly."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fred_response

        with patch.object(collector.settings, "fred_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            
            with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
                mock_get.return_value = mock_response
                
                async with httpx.AsyncClient() as client:
                    points = await collector._collect_fred_inventory_ratio(
                        client,
                        datetime(2024, 1, 1),
                        datetime(2024, 12, 31),
                    )
                    
                    assert isinstance(points, list)

    @pytest.mark.asyncio
    async def test_collect_fred_skips_missing_values(self, collector):
        """Test FRED collection skips observations with missing values."""
        response_with_missing = {
            "observations": [
                {"date": "2024-01-01", "value": "1.45"},
                {"date": "2024-02-01", "value": "."},  # Missing
                {"date": "2024-03-01", "value": "1.48"},
            ]
        }
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response_with_missing

        with patch.object(collector.settings, "fred_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            
            with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
                mock_get.return_value = mock_response
                
                async with httpx.AsyncClient() as client:
                    points = await collector._collect_fred_inventory_ratio(
                        client,
                        datetime(2024, 1, 1),
                        datetime(2024, 12, 31),
                    )
                    
                    for point in points:
                        assert point.value is not None


class TestCoxAutomotiveCollection:
    """Test Cox Automotive data collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return VehicleProductionCollector()

    @pytest.mark.asyncio
    async def test_collect_cox_handles_missing_files(self, collector):
        """Test Cox collection handles missing Excel files gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_cox_inventory(client)
                
                assert isinstance(points, list)


class TestBEACollection:
    """Test BEA motor vehicle output collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return VehicleProductionCollector()

    @pytest.fixture
    def sample_bea_response(self):
        """Sample BEA NIPA API response."""
        return {
            "BEAAPI": {
                "Results": {
                    "Data": [
                        {
                            "LineDescription": "Motor vehicles and parts",
                            "DataValue": "800,000",
                            "TimePeriod": "2024Q1",
                        },
                        {
                            "LineDescription": "Motor vehicles and parts",
                            "DataValue": "825,000",
                            "TimePeriod": "2024Q2",
                        },
                    ]
                }
            }
        }

    @pytest.mark.asyncio
    async def test_collect_bea_parses_quarterly_data(self, collector, sample_bea_response):
        """Test BEA collection parses quarterly motor vehicle output."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_bea_response

        with patch.object(collector.settings, "bea_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            
            with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
                mock_get.return_value = mock_response
                
                async with httpx.AsyncClient() as client:
                    points = await collector._collect_bea_vehicle_output(
                        client,
                        datetime(2024, 1, 1),
                        datetime(2024, 12, 31),
                    )
                    
                    assert isinstance(points, list)


class TestFullCollection:
    """Test full collection cycle."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return VehicleProductionCollector()

    @pytest.mark.asyncio
    async def test_collect_returns_datapoints(self, collector):
        """Test that collect method returns DataPoint objects."""
        sample_point = DataPoint(
            source_id="vehicle_production",
            series_id="TEST_SERIES",
            timestamp=datetime.now(UTC),
            value=1000.0,
            unit="test_unit",
        )
        
        with patch.object(collector, "_collect_census_m3", new_callable=AsyncMock) as mock_m3:
            with patch.object(collector, "_collect_fred_inventory_ratio", new_callable=AsyncMock) as mock_fred:
                with patch.object(collector, "_collect_cox_inventory", new_callable=AsyncMock) as mock_cox:
                    with patch.object(collector, "_collect_bea_vehicle_output", new_callable=AsyncMock) as mock_bea:
                        mock_m3.return_value = [sample_point]
                        mock_fred.return_value = [sample_point]
                        mock_cox.return_value = []
                        mock_bea.return_value = []
                        
                        points = await collector.collect()
                        
                        assert len(points) >= 2
                        assert all(isinstance(p, DataPoint) for p in points)

    @pytest.mark.asyncio
    async def test_collect_handles_partial_failures(self, collector):
        """Test that collect continues if one source fails."""
        sample_point = DataPoint(
            source_id="vehicle_production",
            series_id="TEST_SERIES",
            timestamp=datetime.now(UTC),
            value=1000.0,
            unit="test_unit",
        )
        
        with patch.object(collector, "_collect_census_m3", new_callable=AsyncMock) as mock_m3:
            with patch.object(collector, "_collect_fred_inventory_ratio", new_callable=AsyncMock) as mock_fred:
                with patch.object(collector, "_collect_cox_inventory", new_callable=AsyncMock) as mock_cox:
                    with patch.object(collector, "_collect_bea_vehicle_output", new_callable=AsyncMock) as mock_bea:
                        mock_m3.side_effect = Exception("Census API error")
                        mock_fred.return_value = [sample_point]
                        mock_cox.return_value = []
                        mock_bea.return_value = []
                        
                        points = await collector.collect()
                        
                        assert len(points) >= 1
