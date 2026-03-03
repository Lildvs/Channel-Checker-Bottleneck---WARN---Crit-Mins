"""Tests for Critical Minerals collector."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.data_ingestion.collectors.critical_minerals_collector import (
    CriticalMineralsCollector,
    MineralRecord,
)
from src.data_ingestion.file_collector import FileFormat


@pytest.fixture
def collector() -> CriticalMineralsCollector:
    """Create a Critical Minerals collector instance."""
    return CriticalMineralsCollector()


@pytest.fixture
def sample_usgs_df() -> pd.DataFrame:
    """Create sample USGS MCS DataFrame."""
    return pd.DataFrame({
        "Mineral": ["Lithium", "Cobalt", "Nickel", "Graphite", "Iron Ore"],
        "2022": [130000, 170000, 3100000, 1100000, 2500000000],
        "2023": [180000, 190000, 3400000, 1300000, 2600000000],
        "2024": [220000, 200000, 3500000, 1400000, 2550000000],
    })


@pytest.fixture
def sample_comtrade_df() -> pd.DataFrame:
    """Create sample UN Comtrade response DataFrame."""
    return pd.DataFrame({
        "period": ["202301", "202301", "202302", "202302"],
        "reporterCode": ["840", "840", "840", "840"],
        "reporterDesc": ["USA", "USA", "USA", "USA"],
        "partnerCode": ["156", "392", "156", "392"],
        "partnerDesc": ["China", "Japan", "China", "Japan"],
        "flowCode": ["1", "2", "1", "2"],  # 1=import, 2=export
        "primaryValue": [5000000, 1000000, 5500000, 1200000],
        "netWgt": [10000, 2000, 11000, 2500],
    })


@pytest.fixture
def mock_comtrade_response() -> dict:
    """Mock UN Comtrade API response."""
    return {
        "data": [
            {
                "period": "2024",
                "reporterCode": "840",
                "reporterDesc": "USA",
                "partnerCode": "156",
                "partnerDesc": "China",
                "flowCode": "1",
                "primaryValue": 5000000,
                "netWgt": 10000,
            },
            {
                "period": "2024",
                "reporterCode": "840",
                "reporterDesc": "USA",
                "partnerCode": "392",
                "partnerDesc": "Japan",
                "flowCode": "2",
                "primaryValue": 1000000,
                "netWgt": 2000,
            },
        ]
    }


class TestCriticalMineralsCollector:
    """Tests for CriticalMineralsCollector class."""

    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.name == "Critical Minerals"
        assert collector.source_id == "critical_minerals"
        assert collector.timeout == 180.0

    def test_frequency(self, collector):
        """Test frequency property."""
        from src.data_ingestion.base_collector import DataFrequency

        assert collector.frequency == DataFrequency.MONTHLY

    def test_schedule(self, collector):
        """Test schedule cron expression."""
        schedule = collector.get_schedule()
        assert schedule == "0 14 20 * *"  # 20th of month

    def test_critical_minerals_list(self, collector):
        """Test critical minerals list includes key minerals."""
        expected = ["lithium", "cobalt", "nickel", "graphite", "rare_earths"]
        for mineral in expected:
            assert mineral in collector.CRITICAL_MINERALS

    def test_hs_codes_mapping(self, collector):
        """Test HS codes are mapped for key minerals."""
        expected = ["lithium", "cobalt", "nickel", "copper", "rare_earths"]
        for mineral in expected:
            assert mineral in collector.HS_CODES
            assert len(collector.HS_CODES[mineral]) > 0


class TestUSGSDataParsing:
    """Tests for USGS MCS data parsing."""

    def test_parse_usgs_mcs_basic(self, collector, sample_usgs_df):
        """Test basic USGS MCS parsing."""
        data_points = collector._parse_usgs_mcs(sample_usgs_df)

        assert len(data_points) > 0
        minerals_found = {dp.metadata["mineral"] for dp in data_points}
        assert "lithium" in minerals_found
        assert "cobalt" in minerals_found

    def test_parse_usgs_mcs_year_columns(self, collector, sample_usgs_df):
        """Test USGS parsing identifies year columns correctly."""
        data_points = collector._parse_usgs_mcs(sample_usgs_df)

        years = {dp.timestamp.year for dp in data_points}
        assert 2022 in years
        assert 2023 in years
        assert 2024 in years

    def test_parse_usgs_mcs_values(self, collector, sample_usgs_df):
        """Test USGS values are correctly parsed."""
        data_points = collector._parse_usgs_mcs(sample_usgs_df)

        lithium_2024 = [
            dp for dp in data_points
            if dp.metadata["mineral"] == "lithium" and dp.timestamp.year == 2024
        ]

        if lithium_2024:
            assert lithium_2024[0].value == 220000

    def test_parse_usgs_empty_dataframe(self, collector):
        """Test parsing empty DataFrame."""
        empty_df = pd.DataFrame()
        data_points = collector._parse_usgs_mcs(empty_df)
        assert data_points == []

    def test_parse_usgs_filters_non_critical(self, collector, sample_usgs_df):
        """Test that non-critical minerals are filtered out."""
        data_points = collector._parse_usgs_mcs(sample_usgs_df)

        minerals_found = {dp.metadata["mineral"] for dp in data_points}
        assert "iron ore" not in minerals_found


class TestComtradeDataParsing:
    """Tests for UN Comtrade data parsing."""

    def test_parse_comtrade_basic(self, collector, sample_comtrade_df):
        """Test basic Comtrade data parsing."""
        data_points = collector._parse_comtrade_data(sample_comtrade_df, "lithium")

        assert len(data_points) > 0
        assert all(dp.metadata["mineral"] == "lithium" for dp in data_points)

    def test_parse_comtrade_flow_types(self, collector, sample_comtrade_df):
        """Test Comtrade parsing identifies import/export correctly."""
        data_points = collector._parse_comtrade_data(sample_comtrade_df, "cobalt")

        series_ids = {dp.series_id for dp in data_points}
        assert "COMTRADE_COBALT_IMPORT" in series_ids
        assert "COMTRADE_COBALT_EXPORT" in series_ids

    def test_parse_comtrade_metadata(self, collector, sample_comtrade_df):
        """Test Comtrade metadata is correctly extracted."""
        data_points = collector._parse_comtrade_data(sample_comtrade_df, "nickel")

        if data_points:
            dp = data_points[0]
            assert "reporter" in dp.metadata
            assert "partner" in dp.metadata
            assert dp.metadata["source"] == "UN_COMTRADE"

    def test_parse_comtrade_period_format(self, collector):
        """Test parsing different period formats."""
        df_annual = pd.DataFrame({
            "period": ["2024"],
            "reporterCode": ["840"],
            "flowCode": ["1"],
            "primaryValue": [1000000],
        })
        data_points = collector._parse_comtrade_data(df_annual, "copper")
        if data_points:
            assert data_points[0].timestamp.year == 2024


class TestDataframeToDatapoints:
    """Tests for the main parse_dataframe_to_datapoints method."""

    def test_parse_usgs_dataset(self, collector, sample_usgs_df):
        """Test parsing USGS dataset type."""
        data_points = collector.parse_dataframe_to_datapoints(
            sample_usgs_df, "USGS_MCS"
        )
        assert isinstance(data_points, list)

    def test_parse_comtrade_dataset(self, collector, sample_comtrade_df):
        """Test parsing Comtrade dataset type."""
        data_points = collector.parse_dataframe_to_datapoints(
            sample_comtrade_df, "COMTRADE_LITHIUM"
        )
        assert isinstance(data_points, list)

    def test_parse_unknown_dataset(self, collector, sample_usgs_df):
        """Test parsing unknown dataset type."""
        data_points = collector.parse_dataframe_to_datapoints(
            sample_usgs_df, "UNKNOWN_DATASET"
        )
        assert data_points == []


class TestMineralRecord:
    """Tests for MineralRecord dataclass."""

    def test_create_record(self):
        """Test creating a mineral record."""
        from decimal import Decimal

        record = MineralRecord(
            mineral="lithium",
            country="Australia",
            year=2024,
            data_type="production",
            value=Decimal("55000"),
            unit="metric_tons",
            source="USGS_MCS",
        )

        assert record.mineral == "lithium"
        assert record.country == "Australia"
        assert record.value == Decimal("55000")

    def test_record_with_metadata(self):
        """Test record with additional metadata."""
        from decimal import Decimal

        record = MineralRecord(
            mineral="cobalt",
            country="DRC",
            year=2024,
            data_type="production",
            value=Decimal("130000"),
            unit="metric_tons",
            source="USGS_MCS",
            metadata={"purity": "refined", "grade": "high"},
        )

        assert record.metadata["purity"] == "refined"


class TestComtradeAPIIntegration:
    """Integration tests for UN Comtrade API."""

    @pytest.mark.asyncio
    async def test_collect_comtrade_data(self, collector, mock_comtrade_response):
        """Test Comtrade data collection with mocked API."""
        with patch.object(collector.settings, "un_comtrade_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            mock_key.__bool__ = lambda self: True

            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client_class.return_value.__aenter__.return_value = mock_client

                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = mock_comtrade_response
                mock_client.get.return_value = mock_response

                with patch.object(
                    collector.comtrade_limiter, "acquire", new_callable=AsyncMock
                ):
                    data_points = await collector.collect_comtrade_data(
                        "lithium", 2024
                    )

                    assert isinstance(data_points, list)

    @pytest.mark.asyncio
    async def test_collect_comtrade_no_api_key(self, collector):
        """Test Comtrade collection without API key."""
        with patch.object(collector.settings, "un_comtrade_api_key", None):
            data_points = await collector.collect_comtrade_data("lithium", 2024)
            assert data_points == []

    @pytest.mark.asyncio
    async def test_collect_comtrade_unknown_mineral(self, collector):
        """Test Comtrade collection with unknown mineral."""
        with patch.object(collector.settings, "un_comtrade_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"

            data_points = await collector.collect_comtrade_data(
                "unknown_mineral", 2024
            )
            assert data_points == []


class TestAPIValidation:
    """Tests for API key validation."""

    @pytest.mark.asyncio
    async def test_validate_with_api_key(self, collector):
        """Test validation with API key."""
        with patch.object(collector.settings, "un_comtrade_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            mock_key.__bool__ = lambda self: True

            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client_class.return_value.__aenter__.return_value = mock_client

                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_client.get.return_value = mock_response

                is_valid = await collector.validate_api_key()
                assert is_valid is True

    @pytest.mark.asyncio
    async def test_validate_no_api_key(self, collector):
        """Test validation without API key."""
        with patch.object(collector.settings, "un_comtrade_api_key", None):
            is_valid = await collector.validate_api_key()
            assert is_valid is False


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_parse_usgs_nan_values(self, collector):
        """Test parsing USGS data with NaN values."""
        df = pd.DataFrame({
            "Mineral": ["Lithium", "Cobalt"],
            "2024": [220000, None],
        })

        data_points = collector._parse_usgs_mcs(df)
        assert isinstance(data_points, list)

    def test_parse_comtrade_missing_fields(self, collector):
        """Test parsing Comtrade data with missing fields."""
        df = pd.DataFrame({
            "period": ["202401"],
            "flowCode": ["1"],
        })

        data_points = collector._parse_comtrade_data(df, "lithium")
        assert isinstance(data_points, list)

    @pytest.mark.asyncio
    async def test_comtrade_api_error(self, collector):
        """Test handling of Comtrade API errors."""
        with patch.object(collector.settings, "un_comtrade_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            mock_key.__bool__ = lambda self: True

            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client_class.return_value.__aenter__.return_value = mock_client

                mock_response = MagicMock()
                mock_response.status_code = 500
                mock_client.get.return_value = mock_response

                with patch.object(
                    collector.comtrade_limiter, "acquire", new_callable=AsyncMock
                ):
                    data_points = await collector.collect_comtrade_data(
                        "lithium", 2024
                    )

                    assert data_points == []


# =============================================================================
# IEA Critical Minerals Tests
# =============================================================================


@pytest.fixture
def sample_iea_sdmx_csv() -> str:
    """Sample IEA SDMX CSV response."""
    return """DATAFLOW,COMMODITY,SCENARIO,UNIT,TIME_PERIOD,OBS_VALUE
CRITMIN,LI,NZE,kt,2025,190
CRITMIN,LI,NZE,kt,2030,490
CRITMIN,LI,NZE,kt,2040,1100
CRITMIN,CO,NZE,kt,2025,210
CRITMIN,CO,NZE,kt,2030,340
CRITMIN,NI,NZE,kt,2025,3200
CRITMIN,NI,NZE,kt,2030,4100
CRITMIN,CU,NZE,kt,2025,26000
CRITMIN,CU,NZE,kt,2030,32000
"""


class TestIEAConfiguration:
    """Tests for IEA Critical Minerals configuration."""

    def test_iea_minerals_mapping(self, collector):
        """Test IEA minerals mapping exists."""
        assert hasattr(collector, "IEA_MINERALS")
        assert len(collector.IEA_MINERALS) > 0

        expected_codes = ["LI", "CO", "NI", "CU", "REE"]
        for code in expected_codes:
            assert code in collector.IEA_MINERALS

    def test_iea_scenarios_mapping(self, collector):
        """Test IEA scenarios mapping exists."""
        assert hasattr(collector, "IEA_SCENARIOS")
        assert "NZE" in collector.IEA_SCENARIOS
        assert "APS" in collector.IEA_SCENARIOS
        assert "STEPS" in collector.IEA_SCENARIOS

    def test_iea_dataflows(self, collector):
        """Test IEA dataflows are configured."""
        assert hasattr(collector, "IEA_DATAFLOWS")
        assert "CRITMIN" in collector.IEA_DATAFLOWS

    def test_iea_rate_limiter(self, collector):
        """Test IEA rate limiter is configured."""
        assert hasattr(collector, "iea_limiter")


class TestIEADataParsing:
    """Tests for IEA SDMX data parsing."""

    def test_parse_iea_sdmx_response(self, collector, sample_iea_sdmx_csv):
        """Test parsing IEA SDMX CSV response."""
        data_points = collector._parse_iea_sdmx_response(
            sample_iea_sdmx_csv, "CRITMIN"
        )

        assert len(data_points) > 0

        minerals = {dp.metadata["mineral"] for dp in data_points}
        assert "lithium" in minerals
        assert "cobalt" in minerals
        assert "copper" in minerals

    def test_parse_iea_sdmx_scenario(self, collector, sample_iea_sdmx_csv):
        """Test IEA scenario is correctly parsed."""
        data_points = collector._parse_iea_sdmx_response(
            sample_iea_sdmx_csv, "CRITMIN"
        )

        for dp in data_points:
            assert dp.metadata["scenario"] == "NZE"
            assert "scenario_name" in dp.metadata

    def test_parse_iea_sdmx_years(self, collector, sample_iea_sdmx_csv):
        """Test IEA years are correctly parsed."""
        data_points = collector._parse_iea_sdmx_response(
            sample_iea_sdmx_csv, "CRITMIN"
        )

        years = {dp.timestamp.year for dp in data_points}
        assert 2025 in years
        assert 2030 in years
        assert 2040 in years

    def test_parse_iea_sdmx_series_id(self, collector, sample_iea_sdmx_csv):
        """Test IEA series ID format."""
        data_points = collector._parse_iea_sdmx_response(
            sample_iea_sdmx_csv, "CRITMIN"
        )

        for dp in data_points:
            assert dp.series_id.startswith("IEA_")
            assert "NZE" in dp.series_id

    def test_parse_iea_sdmx_empty_response(self, collector):
        """Test parsing empty IEA response."""
        data_points = collector._parse_iea_sdmx_response("", "CRITMIN")
        assert data_points == []


class TestIEAFallback:
    """Tests for IEA fallback data."""

    @pytest.mark.asyncio
    async def test_iea_fallback_returns_data(self, collector):
        """Test IEA fallback returns baseline projections."""
        data_points = await collector._collect_iea_fallback()

        assert len(data_points) > 0

        minerals = {dp.metadata["mineral"] for dp in data_points}
        assert "lithium" in minerals
        assert "cobalt" in minerals
        assert "nickel" in minerals
        assert "copper" in minerals

    @pytest.mark.asyncio
    async def test_iea_fallback_metadata(self, collector):
        """Test IEA fallback includes correct metadata."""
        data_points = await collector._collect_iea_fallback()

        for dp in data_points:
            assert dp.metadata.get("is_fallback") is True
            assert dp.metadata.get("source") == "IEA_CRITICAL_MINERALS"
            assert dp.metadata.get("scenario") == "NZE"

    @pytest.mark.asyncio
    async def test_iea_fallback_years(self, collector):
        """Test IEA fallback covers projection years."""
        data_points = await collector._collect_iea_fallback()

        years = {dp.timestamp.year for dp in data_points}
        assert 2025 in years
        assert 2030 in years
        assert 2040 in years
        assert 2050 in years


class TestIEAAPIIntegration:
    """Integration tests for IEA SDMX API."""

    @pytest.mark.asyncio
    async def test_collect_iea_data_success(self, collector, sample_iea_sdmx_csv):
        """Test IEA data collection with mocked API."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = sample_iea_sdmx_csv
            mock_client.get.return_value = mock_response

            with patch.object(
                collector.iea_limiter, "acquire", new_callable=AsyncMock
            ):
                data_points = await collector.collect_iea_data()

                assert len(data_points) > 0
                assert all(
                    dp.metadata["source"] == "IEA_CRITICAL_MINERALS"
                    for dp in data_points
                )

    @pytest.mark.asyncio
    async def test_collect_iea_data_api_error_uses_fallback(self, collector):
        """Test IEA collection falls back on API error."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_client.get.return_value = mock_response

            with patch.object(
                collector.iea_limiter, "acquire", new_callable=AsyncMock
            ):
                data_points = await collector.collect_iea_data()

                assert len(data_points) > 0
                assert all(
                    dp.metadata.get("is_fallback", False) for dp in data_points
                )

    @pytest.mark.asyncio
    async def test_collect_iea_data_timeout_uses_fallback(self, collector):
        """Test IEA collection falls back on timeout."""
        import httpx

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_client.get.side_effect = httpx.TimeoutException("Timeout")

            with patch.object(
                collector.iea_limiter, "acquire", new_callable=AsyncMock
            ):
                data_points = await collector.collect_iea_data()

                assert len(data_points) > 0

    @pytest.mark.asyncio
    async def test_collect_iea_with_scenario_filter(self, collector, sample_iea_sdmx_csv):
        """Test IEA data collection with scenario filter."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = sample_iea_sdmx_csv
            mock_client.get.return_value = mock_response

            with patch.object(
                collector.iea_limiter, "acquire", new_callable=AsyncMock
            ):
                data_points = await collector.collect_iea_data(scenario="NZE")

                assert len(data_points) > 0

                call_kwargs = mock_client.get.call_args
                params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
                assert "c[SCENARIO]" in params


class TestFullCollection:
    """Tests for full collection cycle including IEA."""

    @pytest.mark.asyncio
    async def test_collect_includes_iea(self, collector, sample_iea_sdmx_csv):
        """Test that full collection includes IEA data."""
        with patch.object(collector, "collect_iea_data") as mock_iea:
            mock_iea.return_value = [
                MagicMock(metadata={"source": "IEA_CRITICAL_MINERALS"})
            ]

            with patch.object(collector.settings, "un_comtrade_api_key", None):
                with patch("httpx.AsyncClient") as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client_class.return_value.__aenter__.return_value = mock_client

                    with patch.object(
                        collector.__class__.__bases__[0],  # FileBasedCollector
                        "collect",
                        new_callable=AsyncMock,
                        return_value=[],
                    ):
                        await collector.collect()

                        mock_iea.assert_called_once()
