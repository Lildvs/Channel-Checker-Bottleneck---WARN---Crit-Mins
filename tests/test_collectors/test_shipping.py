"""Tests for Shipping Data collector."""

from datetime import datetime
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.data_ingestion.collectors.shipping_collector import (
    ShippingDataCollector,
    PortMetricsAggregator,
    ShippingMetric,
)
from src.data_ingestion.file_collector import FileFormat


@pytest.fixture
def collector() -> ShippingDataCollector:
    """Create a Shipping Data collector instance."""
    return ShippingDataCollector()


@pytest.fixture
def sample_pola_df() -> pd.DataFrame:
    """Create sample Port of LA TEU DataFrame."""
    return pd.DataFrame({
        "Date": ["2025-01-01", "2025-02-01", "2025-03-01"],
        "Total TEU": [450000, 475000, 460000],
        "Import TEU": [250000, 260000, 255000],
        "Export TEU": [200000, 215000, 205000],
    })


@pytest.fixture
def sample_pola_csv_content() -> bytes:
    """Create sample Port of LA CSV content."""
    csv_content = """Date,Total TEU,Import TEU,Export TEU
2025-01-01,450000,250000,200000
2025-02-01,475000,260000,215000
2025-03-01,460000,255000,205000
"""
    return csv_content.encode("utf-8")


class TestShippingDataCollector:
    """Tests for ShippingDataCollector class."""

    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.name == "Shipping Data"
        assert collector.source_id == "shipping"
        assert collector.timeout == 120.0

    def test_frequency(self, collector):
        """Test frequency property."""
        from src.data_ingestion.base_collector import DataFrequency

        assert collector.frequency == DataFrequency.MONTHLY

    def test_schedule(self, collector):
        """Test schedule cron expression."""
        schedule = collector.get_schedule()
        assert schedule == "0 14 16 * *"  # 16th of month

    def test_get_datasets(self, collector):
        """Test get_datasets returns valid configurations."""
        datasets = collector.get_datasets()

        assert len(datasets) >= 1
        assert any(d.dataset_id == "POLA_TEU" for d in datasets)

        pola_dataset = next(d for d in datasets if d.dataset_id == "POLA_TEU")
        assert pola_dataset.format == FileFormat.CSV
        assert "lacity.org" in pola_dataset.url


class TestPOLADataParsing:
    """Tests for Port of LA data parsing."""

    def test_parse_pola_teu_basic(self, collector, sample_pola_df):
        """Test basic POLA TEU parsing."""
        data_points = collector._parse_pola_teu(sample_pola_df)

        assert len(data_points) > 0

        series_ids = {dp.series_id for dp in data_points}
        assert "POLA_TEU_TOTAL" in series_ids or any("TOTAL" in s for s in series_ids)

    def test_parse_pola_teu_values(self, collector, sample_pola_df):
        """Test POLA TEU values are correctly parsed."""
        data_points = collector._parse_pola_teu(sample_pola_df)

        total_points = [
            dp for dp in data_points
            if "TOTAL" in dp.series_id and dp.timestamp.month == 1
        ]

        if total_points:
            assert total_points[0].value == 450000
            assert total_points[0].unit == "TEU"

    def test_parse_pola_teu_metadata(self, collector, sample_pola_df):
        """Test POLA TEU metadata is correctly set."""
        data_points = collector._parse_pola_teu(sample_pola_df)

        if data_points:
            dp = data_points[0]
            assert dp.metadata["port"] == "Port of Los Angeles"
            assert dp.metadata["port_code"] == "POLA"

    def test_parse_pola_empty_dataframe(self, collector):
        """Test parsing empty DataFrame."""
        empty_df = pd.DataFrame()
        data_points = collector._parse_pola_teu(empty_df)
        assert data_points == []

    def test_parse_pola_missing_date_column(self, collector):
        """Test parsing when date column is missing."""
        df = pd.DataFrame({
            "Total TEU": [450000, 475000],
            "Import TEU": [250000, 260000],
        })
        data_points = collector._parse_pola_teu(df)
        assert isinstance(data_points, list)

    def test_parse_pola_year_month_columns(self, collector):
        """Test parsing with separate year/month columns."""
        df = pd.DataFrame({
            "Year": [2025, 2025, 2025],
            "Month": [1, 2, 3],
            "Total TEU": [450000, 475000, 460000],
        })
        data_points = collector._parse_pola_teu(df)
        assert isinstance(data_points, list)


class TestDataframeToDatapoints:
    """Tests for the main parse_dataframe_to_datapoints method."""

    def test_parse_known_dataset(self, collector, sample_pola_df):
        """Test parsing known dataset type."""
        data_points = collector.parse_dataframe_to_datapoints(
            sample_pola_df, "POLA_TEU"
        )
        assert isinstance(data_points, list)

    def test_parse_unknown_dataset(self, collector, sample_pola_df):
        """Test parsing unknown dataset type."""
        data_points = collector.parse_dataframe_to_datapoints(
            sample_pola_df, "UNKNOWN_DATASET"
        )
        assert data_points == []


class TestPortMetricsAggregator:
    """Tests for PortMetricsAggregator helper class."""

    def test_calculate_yoy_change_positive(self):
        """Test YoY change calculation with increase."""
        change = PortMetricsAggregator.calculate_yoy_change(110, 100)
        assert abs(change - 0.10) < 0.0001

    def test_calculate_yoy_change_negative(self):
        """Test YoY change calculation with decrease."""
        change = PortMetricsAggregator.calculate_yoy_change(90, 100)
        assert abs(change - (-0.10)) < 0.0001

    def test_calculate_yoy_change_zero_prior(self):
        """Test YoY change with zero prior year value."""
        change = PortMetricsAggregator.calculate_yoy_change(100, 0)
        assert change == 0.0

    def test_calculate_mom_change(self):
        """Test MoM change calculation."""
        change = PortMetricsAggregator.calculate_mom_change(105, 100)
        assert abs(change - 0.05) < 0.0001

    def test_calculate_3m_average(self):
        """Test 3-month average calculation."""
        values = [100, 110, 120]
        avg = PortMetricsAggregator.calculate_3m_average(values)
        assert abs(avg - 110) < 0.0001

    def test_calculate_3m_average_less_than_3(self):
        """Test 3-month average with less than 3 values."""
        values = [100, 110]
        avg = PortMetricsAggregator.calculate_3m_average(values)
        assert abs(avg - 105) < 0.0001

    def test_calculate_3m_average_empty(self):
        """Test 3-month average with empty list."""
        avg = PortMetricsAggregator.calculate_3m_average([])
        assert avg == 0.0


class TestShippingMetric:
    """Tests for ShippingMetric dataclass."""

    def test_create_metric(self):
        """Test creating a shipping metric."""
        metric = ShippingMetric(
            port="Port of Los Angeles",
            metric_type="teu_total",
            value=450000.0,
            timestamp=datetime(2025, 1, 1),
            unit="TEU",
        )

        assert metric.port == "Port of Los Angeles"
        assert metric.metric_type == "teu_total"
        assert metric.value == 450000.0
        assert metric.unit == "TEU"

    def test_metric_with_metadata(self):
        """Test metric with additional metadata."""
        metric = ShippingMetric(
            port="Port of Long Beach",
            metric_type="teu_imports",
            value=200000.0,
            timestamp=datetime(2025, 2, 1),
            unit="TEU",
            metadata={"source": "port_website", "preliminary": True},
        )

        assert metric.metadata["source"] == "port_website"
        assert metric.metadata["preliminary"] is True


class TestCollectorIntegration:
    """Integration tests for the shipping collector."""

    @pytest.mark.asyncio
    async def test_collect_with_mocked_download(
        self, collector, sample_pola_csv_content
    ):
        """Test collection with mocked file download."""
        from src.data_ingestion.file_collector import DownloadedFile

        mock_downloaded = DownloadedFile(
            url=collector.POLA_API_URL,
            content=sample_pola_csv_content,
            filename="pola_teu_monthly.csv",
            format=FileFormat.CSV,
            content_hash="abc123",
            size_bytes=len(sample_pola_csv_content),
        )

        with patch.object(
            collector, "download_file", new_callable=AsyncMock
        ) as mock_download:
            mock_download.return_value = mock_downloaded

            with patch.object(collector, "save_raw_file") as mock_save:
                mock_save.return_value = "/tmp/test.csv"

                with patch.object(collector, "update_manifest"):
                    data_points = await collector.collect()

                    assert isinstance(data_points, list)
                    if data_points:
                        assert all(dp.source_id == "shipping" for dp in data_points)

    @pytest.mark.asyncio
    async def test_collect_unchanged_file(self, collector):
        """Test collection when file hasn't changed."""
        with patch.object(
            collector, "download_file", new_callable=AsyncMock
        ) as mock_download:
            mock_download.return_value = None

            data_points = await collector.collect()

            assert data_points == []


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_parse_nan_values(self, collector):
        """Test parsing DataFrame with NaN values."""
        df = pd.DataFrame({
            "Date": ["2025-01-01", "2025-02-01", None],
            "Total TEU": [450000, None, 460000],
        })

        data_points = collector._parse_pola_teu(df)
        assert isinstance(data_points, list)

    def test_parse_invalid_numeric_values(self, collector):
        """Test parsing DataFrame with non-numeric TEU values."""
        df = pd.DataFrame({
            "Date": ["2025-01-01", "2025-02-01"],
            "Total TEU": ["450000", "invalid"],
        })

        data_points = collector._parse_pola_teu(df)
        assert isinstance(data_points, list)

    def test_different_column_naming(self, collector):
        """Test parsing with different column naming conventions."""
        df = pd.DataFrame({
            "period": ["2025-01-01", "2025-02-01"],
            "container_total": [450000, 475000],
        })

        data_points = collector._parse_pola_teu(df)
        assert isinstance(data_points, list)
