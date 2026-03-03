"""Tests for Commodity Inventory collector."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.data_ingestion.collectors.commodity_inventory_collector import (
    CommodityInventoryCollector,
    InventoryRecord,
)
from src.data_ingestion.collectors.commodity_inventory_file_collector import (
    CommodityInventoryFileCollector,
    LME_METALS,
    CME_METALS,
    ALL_METAL_CONFIGS,
    get_commodity_inventory_file_collector,
    get_lme_collector,
    get_comex_collector,
)
from src.data_ingestion.base_collector import DataFrequency


@pytest.fixture
def collector() -> CommodityInventoryCollector:
    """Create a Commodity Inventory collector instance."""
    return CommodityInventoryCollector()


@pytest.fixture
def mock_eia_response() -> dict:
    """Mock EIA API response for petroleum stocks."""
    return {
        "response": {
            "data": [
                {
                    "period": "2025-01-15",
                    "value": 425000,
                    "series-description": "U.S. Crude Oil Stocks",
                },
                {
                    "period": "2025-01-08",
                    "value": 423500,
                    "series-description": "U.S. Crude Oil Stocks",
                },
                {
                    "period": "2025-01-01",
                    "value": 422000,
                    "series-description": "U.S. Crude Oil Stocks",
                },
            ]
        }
    }


@pytest.fixture
def mock_nass_response() -> dict:
    """Mock NASS QuickStats API response for grain stocks."""
    return {
        "data": [
            {
                "year": 2025,
                "reference_period_desc": "MAR",
                "Value": "1,234,567",
                "location_desc": "US TOTAL",
                "commodity_desc": "CORN",
            },
            {
                "year": 2024,
                "reference_period_desc": "DEC",
                "Value": "1,456,789",
                "location_desc": "US TOTAL",
                "commodity_desc": "CORN",
            },
        ]
    }


class TestCommodityInventoryCollector:
    """Tests for CommodityInventoryCollector class."""

    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.name == "Commodity Inventory"
        assert collector.source_id == "commodity_inventory"
        assert len(collector.EIA_SERIES) > 0
        assert len(collector.NASS_COMMODITIES) > 0

    def test_frequency(self, collector):
        """Test frequency property."""
        from src.data_ingestion.base_collector import DataFrequency

        assert collector.frequency == DataFrequency.WEEKLY

    def test_schedule(self, collector):
        """Test schedule cron expression."""
        schedule = collector.get_schedule()
        assert schedule == "30 15 * * 3"  # Wednesday

    def test_default_series(self, collector):
        """Test default series list."""
        series = collector.get_default_series()
        assert len(series) > 0
        assert any("STEO" in s or "PET" in s for s in series)
        assert "CORN" in series or any("CORN" in s for s in series)


class TestEIASeriesConfiguration:
    """Tests for EIA series configuration."""

    def test_eia_series_structure(self, collector):
        """Test EIA series configuration structure."""
        for series_id, config in collector.EIA_SERIES.items():
            assert "name" in config
            assert "commodity" in config
            assert "commodity_type" in config
            assert "unit" in config
            assert config["commodity_type"] == "petroleum"

    def test_eia_series_ids_format(self, collector):
        """Test EIA series IDs have correct format."""
        for series_id in collector.EIA_SERIES:
            parts = series_id.split(".")
            assert len(parts) >= 2
            assert parts[0] in ("PET", "STEO")


class TestNASSConfiguration:
    """Tests for NASS commodity configuration."""

    def test_nass_commodities_structure(self, collector):
        """Test NASS commodities configuration structure."""
        for commodity, config in collector.NASS_COMMODITIES.items():
            assert "name" in config
            assert "commodity" in config
            assert "commodity_type" in config
            assert "unit" in config
            assert config["commodity_type"] == "grain"

    def test_nass_commodities_list(self, collector):
        """Test expected commodities are configured."""
        expected = ["CORN", "SOYBEANS", "WHEAT"]
        for commodity in expected:
            assert commodity in collector.NASS_COMMODITIES


class TestLMEMetalsConfiguration:
    """Tests for LME metals configuration."""

    def test_lme_metals_structure(self, collector):
        """Test LME metals configuration structure."""
        for metal, config in collector.LME_METALS.items():
            assert "symbol" in config
            assert "unit" in config
            assert config["unit"] == "metric_tons"

    def test_lme_metals_list(self, collector):
        """Test expected metals are configured."""
        expected = ["copper", "aluminum", "zinc", "nickel", "lead", "tin"]
        for metal in expected:
            assert metal in collector.LME_METALS


class TestEIACollection:
    """Tests for EIA data collection."""

    @pytest.mark.asyncio
    async def test_collect_eia_petroleum(self, collector, mock_eia_response):
        """Test EIA petroleum data collection."""
        with patch.object(collector.settings, "eia_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"

            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()

                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = mock_eia_response
                mock_client.get.return_value = mock_response

                with patch.object(
                    collector.eia_limiter, "acquire", new_callable=AsyncMock
                ):
                    data_points = await collector._collect_eia_petroleum(
                        mock_client,
                        datetime(2025, 1, 1),
                        datetime(2025, 1, 31),
                    )

                assert isinstance(data_points, list)

    @pytest.mark.asyncio
    async def test_collect_eia_no_api_key(self, collector):
        """Test EIA collection fails gracefully without API key."""
        with patch.object(collector.settings, "eia_api_key", None):
            mock_client = AsyncMock()

            data_points = await collector._collect_eia_petroleum(
                mock_client,
                datetime(2025, 1, 1),
                datetime(2025, 1, 31),
            )

            assert data_points == []


class TestNASSCollection:
    """Tests for NASS data collection."""

    @pytest.mark.asyncio
    async def test_collect_nass_grain_stocks(self, collector, mock_nass_response):
        """Test NASS grain stocks data collection."""
        with patch.object(collector.settings, "usda_nass_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"

            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()

                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = mock_nass_response
                mock_client.get.return_value = mock_response

                with patch.object(
                    collector.nass_limiter, "acquire", new_callable=AsyncMock
                ):
                    data_points = await collector._collect_nass_grain_stocks(
                        mock_client,
                        datetime(2024, 1, 1),
                        datetime(2025, 12, 31),
                    )

                assert isinstance(data_points, list)

    @pytest.mark.asyncio
    async def test_collect_nass_no_api_key(self, collector):
        """Test NASS collection fails gracefully without API key."""
        with patch.object(collector.settings, "usda_nass_api_key", None):
            mock_client = AsyncMock()

            data_points = await collector._collect_nass_grain_stocks(
                mock_client,
                datetime(2025, 1, 1),
                datetime(2025, 1, 31),
            )

            assert data_points == []


class TestFullCollection:
    """Tests for full collection cycle."""

    @pytest.mark.asyncio
    async def test_collect_all_sources(
        self, collector, mock_eia_response, mock_nass_response
    ):
        """Test collecting from all sources."""
        with patch.object(collector.settings, "eia_api_key") as mock_eia_key:
            mock_eia_key.get_secret_value.return_value = "test_eia_key"

            with patch.object(collector.settings, "usda_nass_api_key") as mock_nass_key:
                mock_nass_key.get_secret_value.return_value = "test_nass_key"

                with patch("httpx.AsyncClient") as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client_class.return_value.__aenter__.return_value = mock_client

                    def response_side_effect(url, **kwargs):
                        mock_response = MagicMock()
                        mock_response.status_code = 200
                        if "eia.gov" in url:
                            mock_response.json.return_value = mock_eia_response
                        else:
                            mock_response.json.return_value = mock_nass_response
                        return mock_response

                    mock_client.get.side_effect = response_side_effect

                    with patch.object(
                        collector.eia_limiter, "acquire", new_callable=AsyncMock
                    ):
                        with patch.object(
                            collector.nass_limiter, "acquire", new_callable=AsyncMock
                        ):
                            data_points = await collector.collect(
                                start_date=datetime(2024, 1, 1),
                                end_date=datetime(2025, 12, 31),
                            )

                    assert isinstance(data_points, list)


class TestAPIValidation:
    """Tests for API key validation."""

    @pytest.mark.asyncio
    async def test_validate_with_eia_key(self, collector):
        """Test validation with EIA key."""
        with patch.object(collector.settings, "eia_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"
            mock_key.__bool__ = lambda self: True

            with patch.object(collector.settings, "usda_nass_api_key", None):
                with patch("httpx.AsyncClient") as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client_class.return_value.__aenter__.return_value = mock_client

                    mock_response = MagicMock()
                    mock_response.status_code = 200
                    mock_client.get.return_value = mock_response

                    is_valid = await collector.validate_api_key()

                    assert is_valid is True


class TestDataParsing:
    """Tests for data parsing logic."""

    def test_inventory_record_creation(self):
        """Test InventoryRecord dataclass creation."""
        record = InventoryRecord(
            commodity="crude_oil",
            commodity_type="petroleum",
            source="EIA",
            location=None,
            quantity=425000,
            unit="thousand_barrels",
            stock_type="total",
            timestamp=datetime(2025, 1, 15),
            data_delay_days=0,
        )

        assert record.commodity == "crude_oil"
        assert record.commodity_type == "petroleum"
        assert record.quantity == 425000

    def test_nass_value_parsing(self):
        """Test NASS value parsing with commas."""
        value_str = "1,234,567"
        parsed = float(value_str.replace(",", ""))
        assert parsed == 1234567.0

    def test_nass_period_mapping(self, collector):
        """Test NASS period to month mapping."""
        period_months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
            "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
            "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }

        for period, expected_month in period_months.items():
            assert period_months.get(period) == expected_month


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_eia_api_error(self, collector):
        """Test handling of EIA API errors."""
        with patch.object(collector.settings, "eia_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"

            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_client.get.return_value = mock_response

            with patch.object(
                collector.eia_limiter, "acquire", new_callable=AsyncMock
            ):
                data_points = await collector._collect_eia_petroleum(
                    mock_client,
                    datetime(2025, 1, 1),
                    datetime(2025, 1, 31),
                )

            assert data_points == []

    @pytest.mark.asyncio
    async def test_nass_missing_value(self, collector):
        """Test handling of missing NASS values."""
        mock_response = {
            "data": [
                {
                    "year": 2025,
                    "reference_period_desc": "MAR",
                    "Value": "(D)",  # Suppressed data
                    "location_desc": "US TOTAL",
                    "commodity_desc": "CORN",
                },
            ]
        }

        with patch.object(collector.settings, "usda_nass_api_key") as mock_key:
            mock_key.get_secret_value.return_value = "test_key"

            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = mock_response
            mock_client.get.return_value = mock_resp

            with patch.object(
                collector.nass_limiter, "acquire", new_callable=AsyncMock
            ):
                data_points = await collector._collect_nass_grain_stocks(
                    mock_client,
                    datetime(2025, 1, 1),
                    datetime(2025, 12, 31),
                )

            assert len(data_points) == 0

    @pytest.mark.asyncio
    async def test_empty_date_range(self, collector):
        """Test collection with empty date range."""
        start = datetime(2025, 12, 31)
        end = datetime(2025, 1, 1)

        with patch.object(collector.settings, "eia_api_key", None):
            with patch.object(collector.settings, "usda_nass_api_key", None):
                with patch("httpx.AsyncClient") as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client_class.return_value.__aenter__.return_value = mock_client

                    data_points = await collector.collect(
                        start_date=start,
                        end_date=end,
                    )

                    assert isinstance(data_points, list)


@pytest.fixture
def file_collector() -> CommodityInventoryFileCollector:
    """Create a CommodityInventoryFileCollector instance."""
    return CommodityInventoryFileCollector()


@pytest.fixture
def lme_collector() -> CommodityInventoryFileCollector:
    """Create LME-only collector."""
    return get_lme_collector()


@pytest.fixture
def comex_collector() -> CommodityInventoryFileCollector:
    """Create COMEX-only collector."""
    return get_comex_collector()


@pytest.fixture
def sample_lme_dataframe() -> pd.DataFrame:
    """Sample LME stock summary data."""
    return pd.DataFrame({
        "Date": ["2026-01-20", "2026-01-21", "2026-01-22"],
        "Copper Stocks": [150000, 148500, 147000],
        "Aluminum Stocks": [800000, 795000, 790000],
        "Zinc Stocks": [120000, 119500, 119000],
    })


@pytest.fixture
def sample_comex_dataframe() -> pd.DataFrame:
    """Sample COMEX registrar report data."""
    return pd.DataFrame({
        "Date": ["2026-01-20", "2026-01-21"],
        "Gold Registered": [22500000, 22400000],
        "Gold Eligible": [10500000, 10600000],
        "Silver Registered": [180000000, 179000000],
        "Silver Eligible": [90000000, 91000000],
    })


class TestCommodityInventoryFileCollector:
    """Tests for CommodityInventoryFileCollector."""

    def test_initialization(self, file_collector):
        """Test file collector initialization."""
        assert file_collector.name == "Metal Inventory (File)"
        assert file_collector.source_id == "metal_inventory_file"

    def test_frequency(self, file_collector):
        """Test frequency is daily."""
        assert file_collector.frequency == DataFrequency.DAILY

    def test_schedule(self, file_collector):
        """Test schedule cron expression."""
        schedule = file_collector.get_schedule()
        assert len(schedule.split()) == 5
        parts = schedule.split()
        assert parts[0] == "0"
        assert parts[1] == "11"

    def test_get_datasets(self, file_collector):
        """Test get_datasets returns configurations."""
        datasets = file_collector.get_datasets()
        assert len(datasets) > 0

        for dataset in datasets:
            assert dataset.dataset_id is not None
            assert dataset.url is not None
            assert dataset.format is not None

    def test_lme_only_collector(self, lme_collector):
        """Test LME-only collector."""
        datasets = lme_collector.get_datasets()
        for dataset in datasets:
            assert dataset.dataset_id.startswith("lme_")

    def test_comex_only_collector(self, comex_collector):
        """Test COMEX-only collector."""
        datasets = comex_collector.get_datasets()
        for dataset in datasets:
            assert dataset.dataset_id.startswith("comex_")


class TestLMEConfiguration:
    """Tests for LME metals configuration."""

    def test_lme_metals_structure(self):
        """Test LME metals configuration structure."""
        assert len(LME_METALS) > 0

        for metal, config in LME_METALS.items():
            assert "lme_code" in config
            assert "name" in config
            assert "unit" in config
            assert config["unit"] == "metric_tons"

    def test_lme_metals_list(self):
        """Test expected LME metals are configured."""
        expected = ["copper", "aluminum", "zinc", "nickel", "lead", "tin"]
        for metal in expected:
            assert metal in LME_METALS

    def test_lme_configs_exist(self):
        """Test LME configurations are in ALL_METAL_CONFIGS."""
        lme_keys = [k for k in ALL_METAL_CONFIGS if k.startswith("lme_")]
        assert len(lme_keys) > 0


class TestCMEConfiguration:
    """Tests for CME/COMEX metals configuration."""

    def test_cme_metals_structure(self):
        """Test CME metals configuration structure."""
        assert len(CME_METALS) > 0

        for metal, config in CME_METALS.items():
            assert "cme_code" in config
            assert "name" in config
            assert "unit" in config

    def test_cme_metals_list(self):
        """Test expected CME metals are configured."""
        expected = ["gold", "silver", "copper_comex"]
        for metal in expected:
            assert metal in CME_METALS

    def test_cme_configs_exist(self):
        """Test COMEX configurations are in ALL_METAL_CONFIGS."""
        comex_keys = [k for k in ALL_METAL_CONFIGS if k.startswith("comex_")]
        assert len(comex_keys) > 0


class TestLMEDataParsing:
    """Tests for LME data parsing."""

    def test_parse_lme_dataframe(self, file_collector, sample_lme_dataframe):
        """Test parsing LME stock data."""
        config = ALL_METAL_CONFIGS.get("lme_copper")
        if config is None:
            pytest.skip("lme_copper config not found")

        data_points = file_collector._parse_lme_data(
            sample_lme_dataframe, config
        )

        assert isinstance(data_points, list)

    def test_parse_lme_empty_dataframe(self, file_collector):
        """Test parsing empty LME data."""
        config = ALL_METAL_CONFIGS.get("lme_copper")
        if config is None:
            pytest.skip("lme_copper config not found")

        empty_df = pd.DataFrame()
        data_points = file_collector._parse_lme_data(empty_df, config)

        assert data_points == []


class TestCOMEXDataParsing:
    """Tests for COMEX data parsing."""

    def test_parse_comex_dataframe(self, file_collector, sample_comex_dataframe):
        """Test parsing COMEX registrar data."""
        config = ALL_METAL_CONFIGS.get("comex_gold")
        if config is None:
            pytest.skip("comex_gold config not found")

        data_points = file_collector._parse_comex_data(
            sample_comex_dataframe, config
        )

        assert isinstance(data_points, list)

    def test_parse_comex_empty_dataframe(self, file_collector):
        """Test parsing empty COMEX data."""
        config = ALL_METAL_CONFIGS.get("comex_gold")
        if config is None:
            pytest.skip("comex_gold config not found")

        empty_df = pd.DataFrame()
        data_points = file_collector._parse_comex_data(empty_df, config)

        assert data_points == []


class TestTimestampExtraction:
    """Tests for timestamp extraction."""

    def test_extract_datetime_from_string(self, file_collector):
        """Test extracting timestamp from date string."""
        df = pd.DataFrame({"Date": ["2026-01-20"], "Value": [100]})
        row = df.iloc[0]

        timestamp = file_collector._extract_timestamp(row, df, 0)

        assert timestamp is not None
        assert isinstance(timestamp, datetime)

    def test_extract_datetime_from_pandas_timestamp(self, file_collector):
        """Test extracting timestamp from pandas Timestamp."""
        df = pd.DataFrame({
            "Date": [pd.Timestamp("2026-01-20")],
            "Value": [100]
        })
        row = df.iloc[0]

        timestamp = file_collector._extract_timestamp(row, df, 0)

        assert timestamp is not None


class TestFactoryFunctions:
    """Tests for factory functions."""

    def test_get_commodity_inventory_file_collector(self):
        """Test main factory function."""
        collector = get_commodity_inventory_file_collector()
        assert isinstance(collector, CommodityInventoryFileCollector)

    def test_get_lme_collector(self):
        """Test LME factory function."""
        collector = get_lme_collector()
        assert isinstance(collector, CommodityInventoryFileCollector)
        assert collector._sources == ["lme"]

    def test_get_comex_collector(self):
        """Test COMEX factory function."""
        collector = get_comex_collector()
        assert isinstance(collector, CommodityInventoryFileCollector)
        assert collector._sources == ["comex"]

    def test_get_with_custom_sources(self):
        """Test factory with custom sources."""
        collector = get_commodity_inventory_file_collector(
            sources=["lme"]
        )
        datasets = collector.get_datasets()
        for dataset in datasets:
            assert "lme" in dataset.dataset_id.lower()


class TestIntegrationWithMainCollector:
    """Tests for integration between main and file collectors."""

    @pytest.mark.asyncio
    async def test_collect_metal_stocks_method_exists(self, collector):
        """Test that main collector has _collect_metal_stocks method."""
        assert hasattr(collector, "_collect_metal_stocks")

    @pytest.mark.asyncio
    async def test_collect_metal_stocks_integration(self, collector):
        """Test metal stocks collection integration."""
        with patch(
            "src.data_ingestion.collectors.commodity_inventory_file_collector.CommodityInventoryFileCollector"
        ) as mock_collector_class:
            mock_file_collector = AsyncMock()
            mock_file_collector.collect.return_value = []
            mock_collector_class.return_value = mock_file_collector

            result = await collector._collect_metal_stocks()

            assert isinstance(result, list)
