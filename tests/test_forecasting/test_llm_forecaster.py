"""Tests for LLM forecaster module."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.analysis.signals import BottleneckCategory, BottleneckSignalData
from src.forecasting.llm_config import (
    LLMConfig,
    get_llm_config,
    estimate_cost,
)
from src.forecasting.prompts import (
    format_duration_prompt,
    format_trajectory_prompt,
    format_binary_prompt,
)


@pytest.fixture
def sample_bottleneck() -> BottleneckSignalData:
    """Create a sample bottleneck for testing."""
    return BottleneckSignalData(
        id=uuid4(),
        category=BottleneckCategory.ENERGY_CRUNCH,
        subcategory="natural_gas",
        severity=0.75,
        confidence=0.85,
        detected_at=datetime.utcnow() - timedelta(days=5),
        affected_sectors=["Manufacturing", "Utilities", "Transportation"],
        source_series=["EIA_NG_PRICE", "FRED_NATURAL_GAS"],
        description="Natural gas supply shortage affecting industrial production",
    )


@pytest.fixture
def mock_llm_response() -> str:
    """Mock LLM response for duration forecast."""
    return """
EXPECTED_DURATION_DAYS: 45
PERCENTILE_25: 30
PERCENTILE_75: 60
PROBABILITY_PERSISTS_30_DAYS: 0.75
PROBABILITY_PERSISTS_60_DAYS: 0.35
PROBABILITY_PERSISTS_90_DAYS: 0.15

REASONING:
Based on historical patterns for energy crunch bottlenecks and the current severe
severity level (75%), this bottleneck is expected to persist for approximately 45 days.
The natural gas supply shortage has affected multiple sectors including Manufacturing,
Utilities, and Transportation. Historical precedents from the 2022 European Energy
Crisis suggest that energy-related bottlenecks typically resolve within 2-3 months
when alternative sourcing and demand reduction measures are implemented.
"""


@pytest.fixture
def mock_trajectory_response() -> str:
    """Mock LLM response for trajectory forecast."""
    return """
DAY_7_SEVERITY: 0.70
DAY_14_SEVERITY: 0.62
DAY_21_SEVERITY: 0.55
DAY_30_SEVERITY: 0.48
EXPECTED_RESOLUTION_DAY: 45
TRAJECTORY_TYPE: improving

REASONING:
The severity is expected to gradually decline as demand reduction measures take effect
and alternative supply sources come online. The trajectory shows steady improvement
with an expected resolution around day 45.
"""


class TestLLMConfig:
    """Tests for LLM configuration."""

    def test_default_config(self):
        """Test default LLM configuration."""
        config = get_llm_config("default")

        assert config.model == "gpt-5"
        assert config.temperature == 0.3
        assert config.timeout == 60

    def test_research_config(self):
        """Test research LLM configuration."""
        config = get_llm_config("research")

        assert config.model == "claude-3-5-sonnet-20241022"
        assert config.temperature == 0.2
        assert config.timeout == 120

    def test_fallback_config(self):
        """Test fallback LLM configuration."""
        config = get_llm_config("fallback")

        assert config.model == "gpt-4o-mini"
        assert config.timeout == 30

    def test_unknown_purpose_uses_default(self):
        """Test that unknown purpose returns default config."""
        config = get_llm_config("unknown_purpose")

        assert config.model == "gpt-5"

    def test_llm_config_dataclass(self):
        """Test LLMConfig dataclass creation."""
        config = LLMConfig(
            model="test-model",
            temperature=0.5,
            timeout=90,
            max_tokens=1000,
        )

        assert config.model == "test-model"
        assert config.temperature == 0.5
        assert config.timeout == 90
        assert config.max_tokens == 1000
        assert config.retry_attempts == 3  # Default


class TestCostEstimation:
    """Tests for cost estimation."""

    def test_gpt4o_cost_estimation(self):
        """Test cost estimation for GPT-4o."""
        cost = estimate_cost("gpt-4o", input_tokens=1000, output_tokens=500)

        # GPT-4o: $2.50/1M input, $10.00/1M output
        expected = (1000 / 1_000_000) * 2.50 + (500 / 1_000_000) * 10.00
        assert abs(cost - expected) < 0.0001

    def test_gpt4o_mini_cost_estimation(self):
        """Test cost estimation for GPT-4o-mini."""
        cost = estimate_cost("gpt-4o-mini", input_tokens=10000, output_tokens=2000)

        # GPT-4o-mini: $0.15/1M input, $0.60/1M output
        expected = (10000 / 1_000_000) * 0.15 + (2000 / 1_000_000) * 0.60
        assert abs(cost - expected) < 0.0001

    def test_claude_cost_estimation(self):
        """Test cost estimation for Claude."""
        cost = estimate_cost(
            "claude-3-5-sonnet-20241022", input_tokens=5000, output_tokens=1000
        )

        # Claude: $3.00/1M input, $15.00/1M output
        expected = (5000 / 1_000_000) * 3.00 + (1000 / 1_000_000) * 15.00
        assert abs(cost - expected) < 0.0001

    def test_unknown_model_uses_default_pricing(self):
        """Test that unknown model uses default GPT-5 pricing."""
        cost = estimate_cost("unknown-model", input_tokens=1000, output_tokens=500)

        expected = (1000 / 1_000_000) * 5.00 + (500 / 1_000_000) * 20.00
        assert abs(cost - expected) < 0.0001


class TestPromptTemplates:
    """Tests for prompt template formatting."""

    def test_duration_prompt_formatting(self, sample_bottleneck):
        """Test duration prompt is formatted correctly."""
        prompt = format_duration_prompt(
            category="energy_crunch",
            severity=0.75,
            confidence=0.85,
            sectors=["Manufacturing", "Utilities"],
            description="Energy shortage",
            detected_at=datetime.utcnow().isoformat(),
            source_series=["EIA_NG_PRICE"],
            research_context="Recent analysis suggests...",
            precedents="2022 Energy Crisis: 270 days",
        )

        assert "Energy Crunch" in prompt
        assert "75%" in prompt
        assert "85%" in prompt
        assert "Manufacturing" in prompt
        assert "EIA_NG_PRICE" in prompt
        assert "Recent analysis suggests" in prompt
        assert "EXPECTED_DURATION_DAYS" in prompt

    def test_trajectory_prompt_formatting(self, sample_bottleneck):
        """Test trajectory prompt is formatted correctly."""
        prompt = format_trajectory_prompt(
            category="energy_crunch",
            severity=0.75,
            confidence=0.85,
            sectors=["Manufacturing"],
            description="Energy shortage",
            age_days=5,
            horizon_days=30,
            research_context="",
        )

        assert "Energy Crunch" in prompt
        assert "75%" in prompt
        assert "5 days" in prompt
        assert "30 days" in prompt
        assert "DAY_7_SEVERITY" in prompt

    def test_binary_prompt_formatting(self):
        """Test binary persistence prompt is formatted correctly."""
        prompt = format_binary_prompt(
            category="supply_disruption",
            severity=0.80,
            sectors=["Semiconductors"],
            description="Chip shortage",
            age_days=10,
            threshold_days=60,
            research_context="Industry reports indicate...",
            base_rates="Historical base rate: 55% persist beyond 60 days",
        )

        assert "Supply Disruption" in prompt
        assert "60 days" in prompt
        assert "Semiconductors" in prompt
        assert "PROBABILITY:" in prompt

    def test_empty_sectors_handled(self):
        """Test that empty sectors list is handled gracefully."""
        prompt = format_duration_prompt(
            category="price_spike",
            severity=0.60,
            confidence=0.70,
            sectors=[],
            description="Price spike",
            detected_at="2026-01-01T00:00:00",
            source_series=[],
            research_context="",
            precedents="",
        )

        assert "Multiple sectors" in prompt

    def test_empty_research_context(self):
        """Test that empty research context shows appropriate message."""
        prompt = format_duration_prompt(
            category="price_spike",
            severity=0.60,
            confidence=0.70,
            sectors=["Energy"],
            description="Price spike",
            detected_at="2026-01-01T00:00:00",
            source_series=[],
            research_context="",
            precedents="",
        )

        assert "No research context available" in prompt


class TestBottleneckForecastBot:
    """Tests for BottleneckForecastBot."""

    @pytest.mark.asyncio
    async def test_bottleneck_to_research_topics(self, sample_bottleneck):
        """Test mapping bottleneck category to research topics."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        bot = BottleneckForecastBot()
        topics = bot._bottleneck_to_research_topics(sample_bottleneck)

        assert "energy" in topics or "macro_geoeconomics" in topics

    def test_historical_precedents_lookup(self, sample_bottleneck):
        """Test historical precedents retrieval."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        bot = BottleneckForecastBot()
        precedents = bot._get_historical_precedents(sample_bottleneck)

        assert "European Energy Crisis" in precedents or "No historical precedents" in precedents

    @pytest.mark.asyncio
    async def test_parse_duration_response(
        self, sample_bottleneck, mock_llm_response
    ):
        """Test parsing of LLM duration response."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        bot = BottleneckForecastBot()
        prediction = bot._parse_duration_response(mock_llm_response, sample_bottleneck)

        assert prediction.prediction_value["expected_duration_days"] == 45
        assert prediction.prediction_value["percentile_25"] == 30
        assert prediction.prediction_value["percentile_75"] == 60
        assert abs(prediction.prediction_value["probability_persists_30_days"] - 0.75) < 0.01
        assert "historical patterns" in prediction.reasoning.lower()

    @pytest.mark.asyncio
    async def test_parse_trajectory_response(self, mock_trajectory_response):
        """Test parsing of LLM trajectory response."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        bot = BottleneckForecastBot()
        prediction = bot._parse_trajectory_response(mock_trajectory_response)

        assert abs(prediction.prediction_value["day_7_severity"] - 0.70) < 0.01
        assert abs(prediction.prediction_value["day_30_severity"] - 0.48) < 0.01
        assert prediction.prediction_value["trajectory_type"] == "improving"

    @pytest.mark.asyncio
    async def test_forecast_duration_with_mocked_llm(
        self, sample_bottleneck, mock_llm_response
    ):
        """Test full duration forecast with mocked LLM."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        with patch(
            "src.forecasting.llm_config.is_llm_available", return_value=True
        ):
            with patch(
                "src.forecasting.llm_forecaster.get_forecasting_llm"
            ) as mock_get_llm:
                mock_llm = MagicMock()
                mock_llm.invoke = AsyncMock(return_value=mock_llm_response)
                mock_get_llm.return_value = mock_llm

                bot = BottleneckForecastBot()
                # Mock research gathering to avoid external calls
                bot.gather_research_context = AsyncMock(return_value="")

                result = await bot.forecast_duration(sample_bottleneck)

                assert result.question.question_type == "duration"
                assert result.prediction.prediction_value["expected_duration_days"] == 45
                assert result.prediction.cost_usd >= 0

    @pytest.mark.asyncio
    async def test_research_context_gathering_fallback(self, sample_bottleneck):
        """Test that research context gathering handles missing aggregator."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        bot = BottleneckForecastBot()

        with patch(
            "src.forecasting.llm_forecaster.ResearchAggregator",
            side_effect=ImportError("not installed"),
        ):
            result = await bot.gather_research_context(sample_bottleneck)
            assert result == ""

    def test_cost_tracking(self):
        """Test that forecast bot tracks costs."""
        from src.forecasting.llm_forecaster import BottleneckForecastBot

        bot = BottleneckForecastBot()

        assert bot.total_cost == 0.0
        assert bot.forecast_count == 0


class TestBottleneckForecasterIntegration:
    """Integration tests for BottleneckForecaster with LLM support."""

    @pytest.mark.asyncio
    async def test_forecaster_statistical_fallback(self, sample_bottleneck):
        """Test that forecaster falls back to statistical when LLM unavailable."""
        from src.forecasting.integration import BottleneckForecaster

        with patch("src.forecasting.llm_config.is_llm_available", return_value=False):
            forecaster = BottleneckForecaster(use_llm=True)
            forecast = await forecaster.forecast_bottleneck_duration(sample_bottleneck)

            assert "statistical" in forecast.model_used.lower()
            assert forecast.prediction["expected_duration_days"] > 0

    @pytest.mark.asyncio
    async def test_forecaster_explicit_statistical(self, sample_bottleneck):
        """Test forcing statistical forecast."""
        from src.forecasting.integration import BottleneckForecaster

        forecaster = BottleneckForecaster(use_llm=True)
        forecast = await forecaster.forecast_bottleneck_duration(
            sample_bottleneck, use_llm=False
        )

        assert "statistical" in forecast.model_used.lower()

    @pytest.mark.asyncio
    async def test_forecaster_severity_trajectory(self, sample_bottleneck):
        """Test severity trajectory forecast."""
        from src.forecasting.integration import BottleneckForecaster

        with patch("src.forecasting.llm_config.is_llm_available", return_value=False):
            forecaster = BottleneckForecaster(use_llm=False)
            forecast = await forecaster.forecast_severity_trajectory(
                sample_bottleneck, horizon_days=30
            )

            assert forecast.forecast_type == "severity_trajectory"
            assert len(forecast.prediction["trajectory"]) > 0

    def test_forecaster_llm_enabled_property(self):
        """Test llm_enabled property."""
        from src.forecasting.integration import BottleneckForecaster

        with patch("src.forecasting.llm_config.is_llm_available", return_value=False):
            forecaster = BottleneckForecaster(use_llm=True)
            assert not forecaster.llm_enabled  # No LLM available

    def test_forecaster_total_llm_cost(self):
        """Test total_llm_cost property."""
        from src.forecasting.integration import BottleneckForecaster

        forecaster = BottleneckForecaster(use_llm=False)
        assert forecaster.total_llm_cost == 0.0


class TestBottleneckQuestion:
    """Tests for BottleneckQuestion adapter."""

    def test_from_bottleneck_duration(self, sample_bottleneck):
        """Test creating duration question from bottleneck."""
        from src.forecasting.llm_forecaster import BottleneckQuestion

        question = BottleneckQuestion.from_bottleneck(
            sample_bottleneck, question_type="duration"
        )

        assert question.question_type == "duration"
        assert "Energy Crunch" in question.title
        assert "persist" in question.title.lower()

    def test_from_bottleneck_binary(self, sample_bottleneck):
        """Test creating binary question from bottleneck."""
        from src.forecasting.llm_forecaster import BottleneckQuestion

        question = BottleneckQuestion.from_bottleneck(
            sample_bottleneck, question_type="binary", threshold_days=60
        )

        assert question.question_type == "binary"
        assert "60 days" in question.title
        assert question.threshold_days == 60

    def test_question_id_matches_bottleneck(self, sample_bottleneck):
        """Test that question ID matches bottleneck ID."""
        from src.forecasting.llm_forecaster import BottleneckQuestion

        question = BottleneckQuestion.from_bottleneck(sample_bottleneck)
        assert question.id == str(sample_bottleneck.id)
