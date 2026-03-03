"""Tests for EnergyFlowsCollector."""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.data_ingestion.collectors.energy_flows_collector import (
    EnergyFlowsCollector,
    EnergyFlowType,
    PETROLEUM_PADD_FLOWS,
    REFINERY_UTILIZATION,
    NATURAL_GAS_FLOWS,
    LNG_EXPORT_TERMINALS,
    get_energy_flows_collector,
)


PETROLEUM_PADD_RESPONSE = {
    "response": {
        "data": [
            {
                "period": "2024-01",
                "value": "25000",
                "series": "MTTMPP1P31",
                "duoarea": "R10-R30",
                "product": "CRUDE",
                "units": "thousand barrels",
            },
            {
                "period": "2024-02",
                "value": "27500",
                "series": "MTTMPP1P31",
                "duoarea": "R10-R30",
                "product": "CRUDE",
                "units": "thousand barrels",
            },
            {
                "period": "2024-01",
                "value": "32000",
                "series": "MTTMPP2P31",
                "duoarea": "R20-R30",
                "product": "CRUDE",
                "units": "thousand barrels",
            },
        ]
    }
}

REFINERY_UTILIZATION_RESPONSE = {
    "response": {
        "data": [
            {
                "period": "2024-01",
                "value": "92.5",
                "series": "MOPUEUS2",
                "duoarea": "NUS",
                "process": "OPU",
                "units": "percent",
            },
            {
                "period": "2024-01",
                "value": "88.3",
                "series": "MOPUEP12",
                "duoarea": "R10",
                "process": "OPU",
                "units": "percent",
            },
            {
                "period": "2024-01",
                "value": "95.1",
                "series": "MOPUEP32",
                "duoarea": "R30",
                "process": "OPU",
                "units": "percent",
            },
        ]
    }
}

NATURAL_GAS_RESPONSE = {
    "response": {
        "data": [
            {
                "period": "2023",
                "value": "15234567",
                "series": "NA1240",
                "units": "million cubic feet",
            },
            {
                "period": "2023",
                "value": "14876543",
                "series": "NA1250",
                "units": "million cubic feet",
            },
        ]
    }
}

LNG_EXPORTS_RESPONSE = {
    "response": {
        "data": [
            {
                "period": "2024-01",
                "value": "432156",
                "series": "N9132US2",
                "units": "million cubic feet",
            },
            {
                "period": "2024-02",
                "value": "456789",
                "series": "N9132US2",
                "units": "million cubic feet",
            },
        ]
    }
}


@pytest.fixture
def mock_settings():
    """Create mock settings with EIA API key."""
    settings = MagicMock()
    settings.eia_api_key = MagicMock()
    settings.eia_api_key.get_secret_value.return_value = "test_api_key"
    return settings


@pytest.fixture
def collector(mock_settings):
    """Create EnergyFlowsCollector with mocked settings."""
    with patch(
        "src.data_ingestion.collectors.energy_flows_collector.get_settings",
        return_value=mock_settings,
    ):
        with patch(
            "src.data_ingestion.collectors.energy_flows_collector.get_rate_limiter"
        ) as mock_limiter:
            mock_limiter.return_value = AsyncMock()
            mock_limiter.return_value.__aenter__ = AsyncMock()
            mock_limiter.return_value.__aexit__ = AsyncMock()
            return EnergyFlowsCollector()


class TestEnergyFlowsCollectorInit:
    """Tests for EnergyFlowsCollector initialization."""

    def test_collector_properties(self, collector):
        """Test collector basic properties."""
        assert collector.name == "EIA Energy Flows"
        assert collector.source_id == "eia_energy_flows"

    def test_frequency(self, collector):
        """Test data frequency."""
        from src.data_ingestion.base_collector import DataFrequency

        assert collector.frequency == DataFrequency.MONTHLY

    def test_schedule(self, collector):
        """Test collection schedule."""
        schedule = collector.get_schedule()
        assert schedule == "0 10 5 * *"  # 5th of month at 10 AM UTC

    def test_default_series(self, collector):
        """Test default series includes all flow types."""
        series = collector.get_default_series()
        assert len(series) > 0

        petroleum_series = [s.series_id for s in PETROLEUM_PADD_FLOWS]
        for s_id in petroleum_series:
            assert s_id in series

        refinery_series = [s.series_id for s in REFINERY_UTILIZATION]
        for s_id in refinery_series:
            assert s_id in series


class TestEnergyFlowsCollectorValidation:
    """Tests for API key validation."""

    async def test_validate_api_key_success(self, collector):
        """Test successful API key validation."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(get=AsyncMock(return_value=mock_response))
            )
            mock_client.return_value.__aexit__ = AsyncMock()

            result = await collector.validate_api_key()
            assert result is True

    async def test_validate_api_key_no_key(self, mock_settings):
        """Test validation with no API key."""
        mock_settings.eia_api_key = None

        with patch(
            "src.data_ingestion.collectors.energy_flows_collector.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "src.data_ingestion.collectors.energy_flows_collector.get_rate_limiter"
            ):
                collector = EnergyFlowsCollector()
                result = await collector.validate_api_key()
                assert result is False


class TestEnergyFlowsCollectorCollect:
    """Tests for data collection."""

    async def test_collect_petroleum_padd_flows(self, collector):
        """Test collecting petroleum PADD flow data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = PETROLEUM_PADD_RESPONSE

        with patch.object(
            collector.rate_limiter, "__aenter__", new_callable=AsyncMock
        ):
            with patch.object(
                collector.rate_limiter, "__aexit__", new_callable=AsyncMock
            ):
                with patch("httpx.AsyncClient") as mock_client:
                    mock_instance = AsyncMock()
                    mock_instance.get = AsyncMock(return_value=mock_response)
                    mock_client.return_value.__aenter__ = AsyncMock(
                        return_value=mock_instance
                    )
                    mock_client.return_value.__aexit__ = AsyncMock()

                    data_points = await collector.collect_petroleum_padd_flows()

                    assert len(data_points) > 0
                    assert all(
                        "PETROLEUM_PADD" in dp.series_id for dp in data_points
                    )

    async def test_collect_refinery_utilization(self, collector):
        """Test collecting refinery utilization data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = REFINERY_UTILIZATION_RESPONSE

        with patch.object(
            collector.rate_limiter, "__aenter__", new_callable=AsyncMock
        ):
            with patch.object(
                collector.rate_limiter, "__aexit__", new_callable=AsyncMock
            ):
                with patch("httpx.AsyncClient") as mock_client:
                    mock_instance = AsyncMock()
                    mock_instance.get = AsyncMock(return_value=mock_response)
                    mock_client.return_value.__aenter__ = AsyncMock(
                        return_value=mock_instance
                    )
                    mock_client.return_value.__aexit__ = AsyncMock()

                    data_points = await collector.collect_refinery_utilization()

                    assert len(data_points) > 0
                    assert all(
                        "REFINERY_UTIL" in dp.series_id for dp in data_points
                    )

    async def test_collect_with_date_filters(self, collector):
        """Test collection with date filters."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = PETROLEUM_PADD_RESPONSE

        with patch.object(
            collector.rate_limiter, "__aenter__", new_callable=AsyncMock
        ):
            with patch.object(
                collector.rate_limiter, "__aexit__", new_callable=AsyncMock
            ):
                with patch("httpx.AsyncClient") as mock_client:
                    mock_instance = AsyncMock()
                    mock_instance.get = AsyncMock(return_value=mock_response)
                    mock_client.return_value.__aenter__ = AsyncMock(
                        return_value=mock_instance
                    )
                    mock_client.return_value.__aexit__ = AsyncMock()

                    start_date = datetime(2024, 1, 1)
                    end_date = datetime(2024, 12, 31)

                    data_points = await collector.collect(
                        start_date=start_date, end_date=end_date
                    )

                    call_args = mock_instance.get.call_args
                    assert "start" in call_args.kwargs.get("params", {})

    async def test_collect_no_api_key(self, mock_settings):
        """Test collection with no API key returns empty list."""
        mock_settings.eia_api_key = None

        with patch(
            "src.data_ingestion.collectors.energy_flows_collector.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "src.data_ingestion.collectors.energy_flows_collector.get_rate_limiter"
            ):
                collector = EnergyFlowsCollector()
                data_points = await collector.collect()
                assert data_points == []

    async def test_collect_api_error(self, collector):
        """Test collection handles API errors gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch.object(
            collector.rate_limiter, "__aenter__", new_callable=AsyncMock
        ):
            with patch.object(
                collector.rate_limiter, "__aexit__", new_callable=AsyncMock
            ):
                with patch("httpx.AsyncClient") as mock_client:
                    mock_instance = AsyncMock()
                    mock_instance.get = AsyncMock(return_value=mock_response)
                    mock_client.return_value.__aenter__ = AsyncMock(
                        return_value=mock_instance
                    )
                    mock_client.return_value.__aexit__ = AsyncMock()

                    data_points = await collector.collect()

                    assert data_points == []


class TestEnergyFlowsDataPointParsing:
    """Tests for data point parsing."""

    def test_parse_observation_petroleum(self, collector):
        """Test parsing petroleum observation."""
        obs = {
            "period": "2024-01",
            "value": "25000",
            "series": "MTTMPP1P31",
            "duoarea": "R10-R30",
            "product": "CRUDE",
            "units": "thousand barrels",
        }

        data_point = collector._parse_observation(
            obs, EnergyFlowType.PETROLEUM_PADD, ["MTTMPP1P31"]
        )

        assert data_point is not None
        assert data_point.value == 25000.0
        assert "PETROLEUM_PADD" in data_point.series_id
        assert data_point.timestamp == datetime(2024, 1, 1)

    def test_parse_observation_refinery(self, collector):
        """Test parsing refinery utilization observation."""
        obs = {
            "period": "2024-06",
            "value": "92.5",
            "series": "MOPUEUS2",
            "duoarea": "NUS",
            "process": "OPU",
            "units": "percent",
        }

        data_point = collector._parse_observation(
            obs, EnergyFlowType.REFINERY_UTIL, ["MOPUEUS2"]
        )

        assert data_point is not None
        assert data_point.value == 92.5
        assert "REFINERY_UTIL" in data_point.series_id

    def test_parse_observation_null_value(self, collector):
        """Test parsing observation with null value."""
        obs = {
            "period": "2024-01",
            "value": None,
            "series": "MTTMPP1P31",
        }

        data_point = collector._parse_observation(
            obs, EnergyFlowType.PETROLEUM_PADD, ["MTTMPP1P31"]
        )

        assert data_point is None

    def test_parse_observation_invalid_value(self, collector):
        """Test parsing observation with invalid value."""
        obs = {
            "period": "2024-01",
            "value": "not_a_number",
            "series": "MTTMPP1P31",
        }

        data_point = collector._parse_observation(
            obs, EnergyFlowType.PETROLEUM_PADD, ["MTTMPP1P31"]
        )

        assert data_point is None

    def test_parse_observation_various_date_formats(self, collector):
        """Test parsing observations with various date formats."""
        obs_monthly = {
            "period": "2024-06",
            "value": "100",
            "series": "TEST",
        }
        dp = collector._parse_observation(
            obs_monthly, EnergyFlowType.PETROLEUM_PADD, []
        )
        assert dp is not None
        assert dp.timestamp == datetime(2024, 6, 1)

        obs_annual = {
            "period": "2023",
            "value": "100",
            "series": "TEST",
        }
        dp = collector._parse_observation(
            obs_annual, EnergyFlowType.NATURAL_GAS, []
        )
        assert dp is not None
        assert dp.timestamp == datetime(2023, 1, 1)

        obs_daily = {
            "period": "2024-06-15",
            "value": "100",
            "series": "TEST",
        }
        dp = collector._parse_observation(
            obs_daily, EnergyFlowType.PETROLEUM_PADD, []
        )
        assert dp is not None
        assert dp.timestamp == datetime(2024, 6, 15)


class TestEnergyFlowsFactoryFunction:
    """Tests for factory function."""

    def test_get_energy_flows_collector_singleton(self, mock_settings):
        """Test factory function returns singleton."""
        with patch(
            "src.data_ingestion.collectors.energy_flows_collector.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "src.data_ingestion.collectors.energy_flows_collector.get_rate_limiter"
            ):
                import src.data_ingestion.collectors.energy_flows_collector as module

                module._collector = None

                collector1 = get_energy_flows_collector()
                collector2 = get_energy_flows_collector()

                assert collector1 is collector2
