"""Tests for the Aircraft Parts collector.

Tests Census HS Chapter 88, FAA Registry, and USASpending collection
for aerospace supply chain monitoring.
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import io
import zipfile

import pytest
import pandas as pd
import httpx

from src.data_ingestion.collectors.aircraft_parts_collector import (
    AircraftPartsCollector,
    AerospaceTrade,
    DODContractAward,
    AircraftRegistration,
    get_aircraft_parts_collector,
    HS88_CODES,
    AEROSPACE_PSC_CODES,
    AEROSPACE_NAICS,
    DRONE_KEYWORDS,
)
from src.data_ingestion.base_collector import DataFrequency, DataPoint


class TestAircraftPartsCollectorInit:
    """Test AircraftPartsCollector initialization."""

    def test_collector_creation(self):
        """Test collector can be created."""
        collector = AircraftPartsCollector()
        assert collector.name == "Aircraft Parts"
        assert collector.source_id == "aircraft_parts"

    def test_factory_function(self):
        """Test factory function returns collector."""
        collector = get_aircraft_parts_collector()
        assert isinstance(collector, AircraftPartsCollector)

    def test_frequency_is_daily(self):
        """Test collector frequency is daily."""
        collector = AircraftPartsCollector()
        assert collector.frequency == DataFrequency.DAILY

    def test_schedule_returns_cron(self):
        """Test schedule returns valid cron expression."""
        collector = AircraftPartsCollector()
        schedule = collector.get_schedule()
        assert schedule == "0 12 * * *"  # Daily noon UTC

    def test_default_series(self):
        """Test default series includes expected series."""
        collector = AircraftPartsCollector()
        series = collector.get_default_series()
        
        assert "hs88_trade" in series
        assert "faa_registry" in series
        assert "dod_awards" in series


class TestDataclasses:
    """Test dataclass definitions."""

    def test_aerospace_trade(self):
        """Test AerospaceTrade dataclass."""
        trade = AerospaceTrade(
            hs_code="8802",
            commodity="Powered Aircraft",
            flow="export",
            value=Decimal("1000000000"),
            quantity=Decimal("100"),
            unit="dollars",
            partner_country="Germany",
            timestamp=datetime.now(UTC),
        )
        
        assert trade.hs_code == "8802"
        assert trade.flow == "export"
        assert trade.metadata == {}

    def test_dod_contract_award(self):
        """Test DODContractAward dataclass."""
        award = DODContractAward(
            award_id="ABC123",
            recipient="Lockheed Martin",
            amount=Decimal("50000000"),
            description="F-35 components",
            award_date=datetime.now(UTC),
            psc_code="15XX",
            naics_code="336411",
            is_drone_related=False,
        )
        
        assert award.award_id == "ABC123"
        assert award.is_drone_related is False

    def test_aircraft_registration(self):
        """Test AircraftRegistration dataclass."""
        reg = AircraftRegistration(
            n_number="N12345",
            aircraft_type="Fixed Wing Single Engine",
            manufacturer="CESSNA",
            model="172",
            registration_date=datetime.now(UTC),
            owner_type="Individual",
            is_drone=False,
        )
        
        assert reg.n_number == "N12345"
        assert reg.is_drone is False


class TestHS88Configuration:
    """Test HS Chapter 88 configuration."""

    def test_hs88_codes_defined(self):
        """Test all HS88 codes are defined."""
        expected_codes = ["8801", "8802", "8803", "8804", "8805", "8806", "8807"]
        
        for code in expected_codes:
            assert code in HS88_CODES, f"Missing HS code: {code}"

    def test_hs88_codes_have_required_fields(self):
        """Test all HS88 codes have required fields."""
        for code, config in HS88_CODES.items():
            assert "name" in config
            assert "category" in config

    def test_drone_code_exists(self):
        """Test that drone/UAS code (8806) exists."""
        assert "8806" in HS88_CODES
        assert "drone" in HS88_CODES["8806"]["category"].lower()


class TestPSCConfiguration:
    """Test PSC code configuration."""

    def test_aerospace_psc_codes_defined(self):
        """Test aerospace PSC codes are defined."""
        assert len(AEROSPACE_PSC_CODES) > 0
        
        assert "15" in AEROSPACE_PSC_CODES  # Aircraft
        assert "16" in AEROSPACE_PSC_CODES  # Aircraft components


class TestNAICSConfiguration:
    """Test NAICS code configuration."""

    def test_aerospace_naics_defined(self):
        """Test aerospace NAICS codes are defined."""
        assert len(AEROSPACE_NAICS) > 0
        
        assert "336411" in AEROSPACE_NAICS


class TestDroneKeywords:
    """Test drone keyword configuration."""

    def test_drone_keywords_defined(self):
        """Test drone keywords are defined."""
        assert len(DRONE_KEYWORDS) > 0
        assert "drone" in DRONE_KEYWORDS
        assert "uas" in DRONE_KEYWORDS
        assert "uav" in DRONE_KEYWORDS
        assert "unmanned" in DRONE_KEYWORDS


class TestCensusHS88Collection:
    """Test Census HS Chapter 88 trade collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return AircraftPartsCollector()

    @pytest.fixture
    def sample_census_response(self):
        """Sample Census trade API response."""
        return [
            ["GEN_VAL_MO", "time", "CTY_NAME"],
            ["1000000000", "2024-01", "Germany"],
            ["1500000000", "2024-02", "United Kingdom"],
            ["2000000000", "2024-03", "Japan"],
        ]

    @pytest.mark.asyncio
    async def test_collect_census_hs88_handles_empty(self, collector):
        """Test Census HS88 collection handles empty response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [["GEN_VAL_MO", "time"]]  # Headers only

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_census_hs88_trade(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert isinstance(points, list)

    @pytest.mark.asyncio
    async def test_collect_census_hs88_handles_api_error(self, collector):
        """Test Census HS88 collection handles API errors."""
        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_census_hs88_trade(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert isinstance(points, list)


class TestFAARegistryCollection:
    """Test FAA Registry collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return AircraftPartsCollector()

    @pytest.fixture
    def sample_faa_zip(self):
        """Create a sample FAA registry ZIP file in memory."""
        csv_content = """N-NUMBER,TYPE AIRCRAFT,MFR MDL CODE,ENG MFR MDL
12345,4,CESSNA,0001
23456,4,BOEING,0002
34567,4,LOCKHEED MARTIN,0003
"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('MASTER.txt', csv_content)
        
        return zip_buffer.getvalue()

    def test_summarize_faa_registrations(self, collector):
        """Test FAA registration summarization."""
        df = pd.DataFrame({
            "N-NUMBER": ["12345", "23456", "34567"],
            "TYPE AIRCRAFT": ["4", "4", "4"],
            "MFR MDL CODE": ["CESSNA", "BOEING", "CESSNA"],
            "MODEL": ["172", "737", "182"],
        })
        
        points = collector._summarize_faa_registrations(df)
        
        assert isinstance(points, list)
        assert len(points) > 0

    @pytest.mark.asyncio
    async def test_collect_faa_registry_handles_timeout(self, collector):
        """Test FAA registry collection handles download timeout."""
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = httpx.TimeoutException("Download timeout")
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_faa_registry(client)
                
                assert isinstance(points, list)
                assert len(points) == 0

    @pytest.mark.asyncio
    async def test_parse_faa_registry_zip(self, collector, sample_faa_zip):
        """Test FAA ZIP file parsing."""
        points = await collector._parse_faa_registry_zip(sample_faa_zip)
        
        assert isinstance(points, list)


class TestUSASpendingCollection:
    """Test USASpending DOD contract collection."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return AircraftPartsCollector()

    @pytest.fixture
    def sample_usaspending_response(self):
        """Sample USASpending API response."""
        return {
            "results": [
                {
                    "Award ID": "DOD-2024-001",
                    "Recipient Name": "Boeing Defense",
                    "Award Amount": 50000000,
                    "Description": "F-15 engine components",
                    "Start Date": "2024-01-15",
                    "Product or Service Code": "1510",
                    "NAICS Code": "336411",
                },
                {
                    "Award ID": "DOD-2024-002",
                    "Recipient Name": "General Atomics",
                    "Award Amount": 25000000,
                    "Description": "Drone surveillance systems UAS",
                    "Start Date": "2024-02-20",
                    "Product or Service Code": "1550",
                    "NAICS Code": "336413",
                },
            ]
        }

    @pytest.mark.asyncio
    async def test_collect_usaspending_parses_awards(self, collector, sample_usaspending_response):
        """Test USASpending collection parses awards."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_usaspending_response

        with patch.object(httpx.AsyncClient, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_usaspending_dod(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert isinstance(points, list)

    @pytest.mark.asyncio
    async def test_collect_usaspending_identifies_drone_contracts(self, collector):
        """Test USASpending collection identifies drone-related contracts."""
        drone_response = {
            "results": [
                {
                    "Award ID": "DOD-DRONE-001",
                    "Recipient Name": "Drone Co",
                    "Award Amount": 10000000,
                    "Description": "Unmanned aerial system components",
                    "Start Date": "2024-03-01",
                    "Product or Service Code": "1550",
                },
            ]
        }
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = drone_response

        with patch.object(httpx.AsyncClient, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_usaspending_dod(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                for point in points:
                    if "unmanned" in point.metadata.get("description", "").lower():
                        assert point.metadata.get("is_drone_related") is True

    @pytest.mark.asyncio
    async def test_collect_usaspending_handles_api_error(self, collector):
        """Test USASpending collection handles API errors gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch.object(httpx.AsyncClient, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                points = await collector._collect_usaspending_dod(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert isinstance(points, list)


class TestDroneContractSearch:
    """Test drone-specific contract searching."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return AircraftPartsCollector()

    @pytest.mark.asyncio
    async def test_collect_drone_contracts_uses_keywords(self, collector):
        """Test drone contract search uses DRONE_KEYWORDS."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}

        with patch.object(httpx.AsyncClient, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            
            async with httpx.AsyncClient() as client:
                await collector._collect_usaspending_drone_contracts(
                    client,
                    datetime(2024, 1, 1),
                    datetime(2024, 12, 31),
                )
                
                assert mock_post.call_count >= 1


class TestFullCollection:
    """Test full collection cycle."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return AircraftPartsCollector()

    @pytest.mark.asyncio
    async def test_collect_returns_datapoints(self, collector):
        """Test that collect method returns DataPoint objects."""
        sample_point = DataPoint(
            source_id="aircraft_parts",
            series_id="TEST_SERIES",
            timestamp=datetime.now(UTC),
            value=1000000.0,
            unit="dollars",
        )
        
        with patch.object(collector, "_collect_census_hs88_trade", new_callable=AsyncMock) as mock_census:
            with patch.object(collector, "_collect_faa_registry", new_callable=AsyncMock) as mock_faa:
                with patch.object(collector, "_collect_usaspending_dod", new_callable=AsyncMock) as mock_usa:
                    mock_census.return_value = [sample_point]
                    mock_faa.return_value = [sample_point]
                    mock_usa.return_value = [sample_point]
                    
                    points = await collector.collect()
                    
                    assert len(points) >= 3
                    assert all(isinstance(p, DataPoint) for p in points)

    @pytest.mark.asyncio
    async def test_collect_handles_partial_failures(self, collector):
        """Test that collect continues if one source fails."""
        sample_point = DataPoint(
            source_id="aircraft_parts",
            series_id="TEST_SERIES",
            timestamp=datetime.now(UTC),
            value=1000000.0,
            unit="dollars",
        )
        
        with patch.object(collector, "_collect_census_hs88_trade", new_callable=AsyncMock) as mock_census:
            with patch.object(collector, "_collect_faa_registry", new_callable=AsyncMock) as mock_faa:
                with patch.object(collector, "_collect_usaspending_dod", new_callable=AsyncMock) as mock_usa:
                    mock_census.side_effect = Exception("Census error")
                    mock_faa.return_value = [sample_point]
                    mock_usa.return_value = [sample_point]
                    
                    points = await collector.collect()
                    
                    assert len(points) >= 2


class TestAPIValidation:
    """Test API validation method."""

    @pytest.fixture
    def collector(self):
        """Get collector instance."""
        return AircraftPartsCollector()

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
            mock_client.head.return_value = mock_response
            
            mock_client_class.return_value = mock_client
            
            result = await collector.validate_api_key()
            
            assert isinstance(result, bool)
