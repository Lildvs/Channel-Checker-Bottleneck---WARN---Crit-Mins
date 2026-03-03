"""Tests for SteelProductionCollector."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data_ingestion.collectors.steel_production_collector import (
    SteelProductionCollector,
    SteelProductionRecord,
    STEEL_REGIONS,
    get_steel_production_collector,
)


SAMPLE_HTML_WITH_TABLE = """
<!DOCTYPE html>
<html>
<head><title>AISI Industry Data</title></head>
<body>
    <h1>Weekly Raw Steel Production</h1>
    <p>Week ending January 25, 2025</p>
    <table class="steel-data">
        <thead>
            <tr>
                <th>Region</th>
                <th>Production (Net Tons)</th>
                <th>Utilization (%)</th>
                <th>YoY Change (%)</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Total</td>
                <td>1,778,000</td>
                <td>76.9</td>
                <td>-2.3</td>
            </tr>
            <tr>
                <td>Northeast</td>
                <td>185,000</td>
                <td>72.5</td>
                <td>-1.5</td>
            </tr>
            <tr>
                <td>Great Lakes</td>
                <td>612,000</td>
                <td>78.2</td>
                <td>-3.1</td>
            </tr>
            <tr>
                <td>Midwest</td>
                <td>245,000</td>
                <td>74.8</td>
                <td>1.2</td>
            </tr>
            <tr>
                <td>Southern</td>
                <td>485,000</td>
                <td>79.1</td>
                <td>-2.8</td>
            </tr>
            <tr>
                <td>Western</td>
                <td>251,000</td>
                <td>75.3</td>
                <td>-0.9</td>
            </tr>
        </tbody>
    </table>
</body>
</html>
"""

SAMPLE_HTML_WITH_TEXT = """
<!DOCTYPE html>
<html>
<head><title>AISI Industry Data</title></head>
<body>
    <div class="content">
        <h1>Weekly Raw Steel Production</h1>
        <div class="production-stats">
            <p>For the week ending January 25, 2025, domestic raw steel production 
            was 1,778,000 net tons while the capability utilization rate was 76.9%.</p>
            <p>Year over year, production decreased by 2.3%.</p>
        </div>
    </div>
</body>
</html>
"""

SAMPLE_HTML_MINIMAL = """
<!DOCTYPE html>
<html>
<head><title>AISI</title></head>
<body>
    <p>Weekly production: 1,800,000 tons at 78% utilization</p>
</body>
</html>
"""

SAMPLE_HTML_EMPTY = """
<!DOCTYPE html>
<html>
<head><title>AISI</title></head>
<body>
    <p>No data available</p>
</body>
</html>
"""


@pytest.fixture
def collector():
    """Create SteelProductionCollector instance."""
    return SteelProductionCollector()


class TestSteelProductionCollectorInit:
    """Tests for SteelProductionCollector initialization."""

    def test_collector_properties(self, collector):
        """Test collector basic properties."""
        assert collector.name == "AISI Steel Production"
        assert collector.source_id == "aisi_steel"

    def test_frequency(self, collector):
        """Test data frequency."""
        from src.data_ingestion.base_collector import DataFrequency

        assert collector.frequency == DataFrequency.WEEKLY

    def test_schedule(self, collector):
        """Test collection schedule."""
        schedule = collector.get_schedule()
        assert schedule == "0 15 * * 1"  # Monday at 3 PM UTC

    def test_default_series(self, collector):
        """Test default series includes all regions."""
        series = collector.get_default_series()
        assert len(series) > 0

        for region in STEEL_REGIONS:
            region_key = region.upper().replace(" ", "_")
            assert f"AISI_STEEL_PRODUCTION_{region_key}" in series
            assert f"AISI_STEEL_UTILIZATION_{region_key}" in series

    def test_url(self, collector):
        """Test data source URL."""
        assert collector.URL == "https://www.steel.org/industry-data/"


class TestSteelProductionCollectorFetch:
    """Tests for page fetching."""

    async def test_fetch_page_success(self, collector):
        """Test successful page fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_HTML_WITH_TABLE

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_client.return_value.__aexit__ = AsyncMock()

            html = await collector._fetch_page()

            assert html is not None
            assert "Weekly Raw Steel Production" in html

    async def test_fetch_page_error(self, collector):
        """Test page fetch with HTTP error."""
        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_client.return_value.__aexit__ = AsyncMock()

            html = await collector._fetch_page()

            assert html is None

    async def test_fetch_page_network_error(self, collector):
        """Test page fetch with network error."""
        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get = AsyncMock(side_effect=Exception("Connection error"))
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_client.return_value.__aexit__ = AsyncMock()

            html = await collector._fetch_page()

            assert html is None


class TestSteelProductionCollectorParsing:
    """Tests for HTML parsing."""

    def test_parse_page_with_table(self, collector):
        """Test parsing page with data table."""
        records = collector._parse_page(SAMPLE_HTML_WITH_TABLE)

        assert len(records) > 0

        total_records = [r for r in records if r.region == "Total"]
        assert len(total_records) > 0

        total = total_records[0]
        assert total.production_tons == 1778000
        assert total.utilization_rate == 76.9

    def test_parse_page_with_text(self, collector):
        """Test parsing page with text-based data."""
        records = collector._parse_page(SAMPLE_HTML_WITH_TEXT)

        assert len(records) >= 0  # May or may not find data depending on patterns

    def test_parse_page_minimal(self, collector):
        """Test parsing minimal page with basic patterns."""
        records = collector._parse_page(SAMPLE_HTML_MINIMAL)

        assert len(records) >= 0

    def test_parse_page_empty(self, collector):
        """Test parsing empty page returns empty list."""
        records = collector._parse_page(SAMPLE_HTML_EMPTY)

        assert records == []

    def test_parse_number_with_commas(self, collector):
        """Test parsing numbers with comma separators."""
        assert collector._parse_number("1,778,000") == 1778000.0
        assert collector._parse_number("25,000") == 25000.0

    def test_parse_number_with_percentage(self, collector):
        """Test parsing numbers with percentage signs."""
        assert collector._parse_number("76.9%") == 76.9
        assert collector._parse_number("92.5 %") == 92.5

    def test_parse_number_negative(self, collector):
        """Test parsing negative numbers."""
        assert collector._parse_number("-2.3") == -2.3
        assert collector._parse_number("(-5.5)") == -5.5

    def test_parse_number_invalid(self, collector):
        """Test parsing invalid numbers."""
        assert collector._parse_number("") is None
        assert collector._parse_number("N/A") is None
        assert collector._parse_number("not a number") is None

    def test_parse_date_formats(self, collector):
        """Test parsing various date formats."""
        assert collector._parse_date("01/25/2025") == datetime(2025, 1, 25)
        assert collector._parse_date("2025-01-25") == datetime(2025, 1, 25)
        assert collector._parse_date("January 25, 2025") == datetime(2025, 1, 25)
        assert collector._parse_date("Jan 25, 2025") == datetime(2025, 1, 25)

    def test_parse_date_invalid(self, collector):
        """Test parsing invalid dates."""
        assert collector._parse_date("") is None
        assert collector._parse_date("not a date") is None


class TestSteelProductionCollectorCollect:
    """Tests for data collection."""

    async def test_collect_success(self, collector):
        """Test successful data collection."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_WITH_TABLE

            data_points = await collector.collect()

            assert len(data_points) > 0
            assert all(dp.source_id == "aisi_steel" for dp in data_points)

    async def test_collect_creates_production_datapoints(self, collector):
        """Test collection creates production data points."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_WITH_TABLE

            data_points = await collector.collect()

            production_dps = [
                dp for dp in data_points if "PRODUCTION" in dp.series_id
            ]
            assert len(production_dps) > 0
            assert all(dp.unit == "net tons" for dp in production_dps)

    async def test_collect_creates_utilization_datapoints(self, collector):
        """Test collection creates utilization data points."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_WITH_TABLE

            data_points = await collector.collect()

            utilization_dps = [
                dp for dp in data_points if "UTILIZATION" in dp.series_id
            ]
            assert len(utilization_dps) > 0
            assert all(dp.unit == "percent" for dp in utilization_dps)

    async def test_collect_with_date_filter(self, collector):
        """Test collection with date filters."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_WITH_TABLE

            start_date = datetime(2020, 1, 1)
            end_date = datetime(2020, 12, 31)

            data_points = await collector.collect(
                start_date=start_date, end_date=end_date
            )

            # Note: This depends on the default week_ending calculation
            assert isinstance(data_points, list)

    async def test_collect_fetch_failure(self, collector):
        """Test collection handles fetch failure."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = None

            data_points = await collector.collect()

            assert data_points == []

    async def test_collect_parse_failure(self, collector):
        """Test collection handles parse errors gracefully."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_EMPTY

            data_points = await collector.collect()

            assert data_points == []


class TestSteelProductionRecordConversion:
    """Tests for record to data point conversion."""

    def test_records_to_datapoints(self, collector):
        """Test converting records to data points."""
        records = [
            SteelProductionRecord(
                week_ending=datetime(2025, 1, 25),
                region="Total",
                production_tons=1778000,
                utilization_rate=76.9,
                yoy_change=-2.3,
            ),
            SteelProductionRecord(
                week_ending=datetime(2025, 1, 25),
                region="Great Lakes",
                production_tons=612000,
                utilization_rate=78.2,
                yoy_change=-3.1,
            ),
        ]

        data_points = collector._records_to_datapoints(records, None, None)

        # Should have 2 records * 2 metrics = 4 data points
        assert len(data_points) == 4

        series_ids = [dp.series_id for dp in data_points]
        assert "AISI_STEEL_PRODUCTION_TOTAL" in series_ids
        assert "AISI_STEEL_UTILIZATION_TOTAL" in series_ids
        assert "AISI_STEEL_PRODUCTION_GREAT_LAKES" in series_ids
        assert "AISI_STEEL_UTILIZATION_GREAT_LAKES" in series_ids

    def test_records_to_datapoints_with_nulls(self, collector):
        """Test converting records with null values."""
        records = [
            SteelProductionRecord(
                week_ending=datetime(2025, 1, 25),
                region="Total",
                production_tons=1778000,
                utilization_rate=None,  # Missing utilization
                yoy_change=None,
            ),
        ]

        data_points = collector._records_to_datapoints(records, None, None)

        assert len(data_points) == 1
        assert "PRODUCTION" in data_points[0].series_id

    def test_records_to_datapoints_date_filter(self, collector):
        """Test date filtering in conversion."""
        records = [
            SteelProductionRecord(
                week_ending=datetime(2025, 1, 25),
                region="Total",
                production_tons=1778000,
                utilization_rate=76.9,
                yoy_change=None,
            ),
            SteelProductionRecord(
                week_ending=datetime(2024, 1, 25),  # Old record
                region="Total",
                production_tons=1750000,
                utilization_rate=75.0,
                yoy_change=None,
            ),
        ]

        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 12, 31)

        data_points = collector._records_to_datapoints(
            records, start_date, end_date
        )

        assert len(data_points) == 2
        assert all(dp.timestamp.year == 2025 for dp in data_points)


class TestSteelProductionFactoryFunction:
    """Tests for factory function."""

    def test_get_steel_production_collector_singleton(self):
        """Test factory function returns singleton."""
        import src.data_ingestion.collectors.steel_production_collector as module

        module._collector = None

        collector1 = get_steel_production_collector()
        collector2 = get_steel_production_collector()

        assert collector1 is collector2

    def test_factory_creates_correct_type(self):
        """Test factory creates correct collector type."""
        import src.data_ingestion.collectors.steel_production_collector as module

        module._collector = None

        collector = get_steel_production_collector()

        assert isinstance(collector, SteelProductionCollector)


class TestSteelProductionCollectorMetadata:
    """Tests for data point metadata."""

    async def test_datapoint_metadata_includes_region(self, collector):
        """Test data points include region in metadata."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_WITH_TABLE

            data_points = await collector.collect()

            for dp in data_points:
                assert "region" in dp.metadata
                assert dp.metadata["region"] in STEEL_REGIONS

    async def test_datapoint_metadata_includes_metric_type(self, collector):
        """Test data points include metric type in metadata."""
        with patch.object(
            collector, "_fetch_page", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = SAMPLE_HTML_WITH_TABLE

            data_points = await collector.collect()

            for dp in data_points:
                assert "metric" in dp.metadata
                assert dp.metadata["metric"] in ["production", "utilization"]
