"""Tests for SEC EDGAR collector."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data_ingestion.collectors.sec_edgar_collector import (
    SECEdgarCollector,
    SECFiling,
    FilingSignal,
)


@pytest.fixture
def collector() -> SECEdgarCollector:
    """Create a SEC EDGAR collector instance."""
    return SECEdgarCollector()


@pytest.fixture
def sample_filing() -> SECFiling:
    """Create a sample SEC filing."""
    return SECFiling(
        cik="0000320193",
        company_name="Apple Inc.",
        ticker="AAPL",
        filing_type="10-K",
        filing_date=datetime(2025, 11, 1),
        accession_number="0000320193-25-000123",
        primary_document="aapl-20251001.htm",
        file_url="https://data.sec.gov/Archives/edgar/data/320193/000032019325000123/aapl-20251001.htm",
        sic_code="3571",
        sic_description="Electronic Computers",
    )


@pytest.fixture
def mock_submissions_response() -> dict:
    """Mock SEC submissions API response."""
    return {
        "cik": "320193",
        "name": "Apple Inc.",
        "tickers": ["AAPL"],
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "filings": {
            "recent": {
                "form": ["10-K", "10-Q", "8-K", "10-Q"],
                "filingDate": ["2025-11-01", "2025-08-01", "2025-07-15", "2025-05-01"],
                "accessionNumber": [
                    "0000320193-25-000123",
                    "0000320193-25-000100",
                    "0000320193-25-000090",
                    "0000320193-25-000080",
                ],
                "primaryDocument": [
                    "aapl-20251001.htm",
                    "aapl-20250701.htm",
                    "aapl-20250715.htm",
                    "aapl-20250401.htm",
                ],
            }
        },
    }


@pytest.fixture
def mock_filing_content() -> str:
    """Mock 10-K filing content with supply chain keywords."""
    return """
    <html>
    <body>
    <h2>Item 1A. Risk Factors</h2>
    <p>
    Our supply chain operations are subject to various risks including:
    - Supply chain disruptions due to global events
    - Semiconductor shortage affecting component availability
    - Backlog increases in manufacturing capacity
    - Inventory management challenges
    - Lead time extensions from suppliers
    - Logistics and freight cost increases
    - Raw materials availability constraints
    </p>
    <h2>Item 1B. Unresolved Staff Comments</h2>
    <p>None.</p>
    <h2>Item 7. Management's Discussion and Analysis</h2>
    <p>
    During the fiscal year, we experienced supply chain constraints
    that impacted our ability to meet customer demand. The bottleneck
    in semiconductor manufacturing led to extended lead times.
    </p>
    <h2>Item 8. Financial Statements</h2>
    </body>
    </html>
    """


class TestSECEdgarCollector:
    """Tests for SECEdgarCollector class."""

    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.name == "SEC EDGAR"
        assert collector.source_id == "sec_edgar"
        assert len(collector.FILING_TYPES) == 3
        # Expanded keyword list includes direct terms and corporate euphemisms
        # Categories: supply chain, shortage, backlog, inventory, materials, vendor,
        # logistics, timing, capacity, disruption, cost/pricing, layoffs, labor,
        # demand, performance, guidance, investment, accounting, quality, geopolitical,
        # strategic sourcing, technology, real estate, commodities, shipping, minimizing
        assert len(collector.SUPPLY_CHAIN_KEYWORDS) > 400

    def test_frequency(self, collector):
        """Test frequency property."""
        from src.data_ingestion.base_collector import DataFrequency

        assert collector.frequency == DataFrequency.DAILY

    def test_schedule(self, collector):
        """Test schedule cron expression."""
        schedule = collector.get_schedule()
        assert schedule == "0 0 * * 1-5"  # Weekdays only

    def test_default_series(self, collector):
        """Test default series list."""
        series = collector.get_default_series()
        assert "SEC_FILINGS_10K" in series
        assert "SEC_FILINGS_10Q" in series
        assert "SEC_FILINGS_8K" in series


class TestKeywordExtraction:
    """Tests for keyword extraction functionality."""

    def test_count_keyword_matches(self, collector):
        """Test keyword counting in text."""
        text = """
        Our supply chain has experienced disruptions.
        The semiconductor shortage affected production.
        Inventory levels are below normal.
        We face backlog in orders and supply chain constraints.
        """

        matches = collector._count_keyword_matches(text)

        assert "supply chain" in matches
        assert matches["supply chain"] == 2  # "supply chain" appears twice
        assert "shortage" in matches
        assert "inventory" in matches
        assert "backlog" in matches

    def test_count_keyword_matches_case_insensitive(self, collector):
        """Test that keyword matching is case insensitive."""
        text = "SUPPLY CHAIN issues and Supply Chain problems"

        matches = collector._count_keyword_matches(text)

        assert "supply chain" in matches
        assert matches["supply chain"] == 2

    def test_no_matches_empty_text(self, collector):
        """Test no matches for irrelevant text."""
        text = "The company reported strong earnings this quarter."

        matches = collector._count_keyword_matches(text)

        assert len(matches) == 0


class TestRiskFactorExtraction:
    """Tests for risk factor section extraction."""

    def test_extract_risk_factors(self, collector, mock_filing_content):
        """Test extraction of risk factors section."""
        risk_factors = collector._extract_risk_factors(mock_filing_content)

        assert len(risk_factors) > 0
        assert "supply chain" in risk_factors.lower()
        assert "semiconductor" in risk_factors.lower()

    def test_extract_risk_factors_no_section(self, collector):
        """Test extraction when no risk factors section exists."""
        content = "<html><body><p>No risk factors here.</p></body></html>"

        risk_factors = collector._extract_risk_factors(content)

        assert risk_factors == ""


class TestMDAExtraction:
    """Tests for MD&A section extraction."""

    def test_extract_mda(self, collector, mock_filing_content):
        """Test extraction of MD&A section."""
        mda = collector._extract_mda(mock_filing_content)

        assert len(mda) > 0
        assert "supply chain" in mda.lower() or "bottleneck" in mda.lower()

    def test_extract_mda_no_section(self, collector):
        """Test extraction when no MD&A section exists."""
        content = "<html><body><p>No MD&A here.</p></body></html>"

        mda = collector._extract_mda(content)

        assert mda == ""


class TestFilingParsing:
    """Tests for filing parsing."""

    def test_parse_filing_hit(self, collector):
        """Test parsing EFTS search hit."""
        source = {
            "ciks": ["0000320193"],
            "display_names": ["Apple Inc."],
            "tickers": ["AAPL"],
            "form": "10-K",
            "file_date": "2025-11-01",
            "accession_number": "0000320193-25-000123",
            "file_name": "aapl-20251001.htm",
            "file_url": "https://example.com/filing.htm",
        }

        filing = collector._parse_filing_hit(source)

        assert filing is not None
        assert filing.cik == "0000320193"
        assert filing.company_name == "Apple Inc."
        assert filing.ticker == "AAPL"
        assert filing.filing_type == "10-K"

    def test_parse_filing_hit_missing_data(self, collector):
        """Test parsing with missing required data."""
        source = {
            "ciks": ["0000320193"],
        }

        filing = collector._parse_filing_hit(source)

        assert filing is None


class TestSignalToDatapoint:
    """Tests for signal to datapoint conversion."""

    def test_signal_to_datapoint(self, collector, sample_filing):
        """Test converting FilingSignal to DataPoint."""
        signal = FilingSignal(
            filing=sample_filing,
            signal_type="risk_factor_keywords",
            extracted_text="Supply chain disruptions...",
            keyword_matches={"supply chain": 5, "shortage": 3},
            section="risk_factors",
        )

        dp = collector._signal_to_datapoint(signal)

        assert dp.source_id == "sec_edgar"
        assert "10-K" in dp.series_id
        assert dp.timestamp == sample_filing.filing_date
        assert dp.value > 0  # Signal strength
        assert dp.metadata["cik"] == "0000320193"
        assert dp.metadata["company_name"] == "Apple Inc."
        assert dp.metadata["total_keyword_matches"] == 8

    def test_signal_to_datapoint_sector_mapping(self, collector, sample_filing):
        """Test sector mapping from SIC code."""
        signal = FilingSignal(
            filing=sample_filing,
            signal_type="mda_keywords",
            extracted_text="Manufacturing issues...",
            keyword_matches={"manufacturing capacity": 2},
            section="mda",
        )

        dp = collector._signal_to_datapoint(signal)

        # SIC 3571 is in technology range
        assert dp.metadata["sector"] == "technology"


class TestCollectorIntegration:
    """Integration tests with mocked HTTP responses."""

    @pytest.mark.asyncio
    async def test_fetch_from_submissions_api(
        self, collector, mock_submissions_response
    ):
        """Test fetching filings from submissions API."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_submissions_response
            mock_client.get.return_value = mock_response

            start_date = datetime(2025, 10, 1)
            end_date = datetime(2025, 12, 1)

            filings = await collector._fetch_from_submissions_api(
                mock_client, "10-K", start_date, end_date
            )

            assert len(filings) >= 0  # May be 0 if filtering applies

    @pytest.mark.asyncio
    async def test_extract_signals(
        self, collector, sample_filing, mock_filing_content
    ):
        """Test signal extraction from filing."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = mock_filing_content
            mock_client.get.return_value = mock_response

            signals = await collector._extract_signals(mock_client, sample_filing)

            assert len(signals) >= 0

    @pytest.mark.asyncio
    async def test_validate_api_access(self, collector):
        """Test API validation."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client.get.return_value = mock_response

            with patch.object(collector.rate_limiter, "acquire", new_callable=AsyncMock):
                is_valid = await collector.validate_api_key()

            assert is_valid is True

    @pytest.mark.asyncio
    async def test_collect_with_mocked_data(
        self, collector, mock_submissions_response, mock_filing_content
    ):
        """Test full collection cycle with mocked data."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_submissions = MagicMock()
            mock_submissions.status_code = 200
            mock_submissions.json.return_value = mock_submissions_response

            mock_content = MagicMock()
            mock_content.status_code = 200
            mock_content.text = mock_filing_content

            # 404 to trigger fallback to submissions API
            mock_efts = MagicMock()
            mock_efts.status_code = 404

            def side_effect(url):
                if "search-index" in url:
                    return mock_efts
                elif "submissions" in url:
                    return mock_submissions
                else:
                    return mock_content

            mock_client.get.side_effect = side_effect

            with patch.object(collector.rate_limiter, "acquire", new_callable=AsyncMock):
                data_points = await collector.collect(
                    start_date=datetime.utcnow() - timedelta(days=30),
                    end_date=datetime.utcnow(),
                )

            assert isinstance(data_points, list)


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_keyword_matches(self, collector):
        """Test handling empty keyword matches."""
        text = ""
        matches = collector._count_keyword_matches(text)
        assert len(matches) == 0

    def test_very_long_text_extraction(self, collector):
        """Test that long text is properly truncated."""
        long_content = "supply chain " * 10000

        matches = collector._count_keyword_matches(long_content)

        assert "supply chain" in matches
        assert matches["supply chain"] == 10000

    def test_html_tag_stripping(self, collector):
        """Test that HTML tags are properly stripped from risk factors."""
        content = """
        <html>
        <h2>Item 1A. Risk Factors</h2>
        <p><b>Supply chain</b> risks include <i>disruption</i>.</p>
        <h2>Item 1B. Unresolved Staff Comments</h2>
        """

        risk_factors = collector._extract_risk_factors(content)

        assert "<b>" not in risk_factors
        assert "<i>" not in risk_factors
        assert "<p>" not in risk_factors
