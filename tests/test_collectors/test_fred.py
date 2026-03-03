"""Tests for FRED collector."""

import pytest
from datetime import datetime

from src.data_ingestion.collectors.fred_collector import FREDCollector


class TestFREDCollector:
    """Tests for FRED data collector."""

    def test_collector_initialization(self):
        """Test collector initializes correctly."""
        collector = FREDCollector()
        assert collector.name == "FRED"
        assert collector.source_id == "fred"

    def test_default_series(self):
        """Test default series list is populated."""
        collector = FREDCollector()
        series = collector.get_default_series()
        assert len(series) > 0
        assert "GDP" in series
        assert "UNRATE" in series

    def test_schedule(self):
        """Test schedule is a valid cron expression."""
        collector = FREDCollector()
        schedule = collector.get_schedule()
        assert schedule is not None
        assert len(schedule.split()) == 5  # Cron has 5 parts

    @pytest.mark.asyncio
    async def test_validate_api_key_without_key(self):
        """Test API validation fails without key."""
        collector = FREDCollector()
        collector.api_key = None
        is_valid = await collector.validate_api_key()
        assert is_valid is False

    def test_quality_score_calculation(self):
        """Test quality score calculation."""
        collector = FREDCollector()

        score = collector.calculate_quality_score(
            is_preliminary=False,
            revision_number=0,
            data_age_days=30,
        )
        assert score == 1.0

        score = collector.calculate_quality_score(
            is_preliminary=True,
            revision_number=0,
            data_age_days=30,
        )
        assert score < 1.0

        score = collector.calculate_quality_score(
            is_preliminary=False,
            revision_number=0,
            data_age_days=2,
        )
        assert score < 1.0
