"""LLM-powered bottleneck forecaster.

This module provides an adapter that bridges the Metaculus forecasting tools
with our bottleneck data models, using existing research collectors for context.
"""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

from src.analysis.signals import BottleneckSignalData
from src.forecasting.llm_config import (
    get_forecasting_llm,
    is_llm_available,
    estimate_cost,
)
from src.forecasting.prompts import (
    format_duration_prompt,
    format_trajectory_prompt,
    format_binary_prompt,
)

logger = structlog.get_logger()


@dataclass
class BottleneckQuestion:
    """Adapts BottleneckSignalData to a forecasting question format."""

    id: str
    title: str
    description: str
    question_type: str  # "binary", "numeric", "duration"
    resolution_criteria: str
    background_info: str
    bottleneck: BottleneckSignalData
    threshold_days: int | None = None  # For binary questions

    @classmethod
    def from_bottleneck(
        cls,
        bottleneck: BottleneckSignalData,
        question_type: str = "duration",
        threshold_days: int = 60,
    ) -> "BottleneckQuestion":
        """Create a question from a bottleneck signal.

        Args:
            bottleneck: The bottleneck signal
            question_type: Type of question to create
            threshold_days: For binary questions, the persistence threshold

        Returns:
            BottleneckQuestion instance
        """
        category = bottleneck.category.value.replace("_", " ").title()
        sectors = ", ".join(bottleneck.affected_sectors[:3]) if bottleneck.affected_sectors else "multiple sectors"

        if question_type == "binary":
            title = f"Will this {category} bottleneck persist beyond {threshold_days} days?"
            resolution_criteria = f"Resolves YES if severity remains above 20% after {threshold_days} days"
        elif question_type == "duration":
            title = f"How long will this {category} bottleneck persist?"
            resolution_criteria = "Duration until severity drops below 20%"
        else:
            title = f"What will be the severity trajectory of this {category} bottleneck?"
            resolution_criteria = "Severity evolution over forecast horizon"

        return cls(
            id=str(bottleneck.id),
            title=title,
            description=bottleneck.description or f"{category} affecting {sectors}",
            question_type=question_type,
            resolution_criteria=resolution_criteria,
            background_info="",  # Will be populated by research
            bottleneck=bottleneck,
            threshold_days=threshold_days if question_type == "binary" else None,
        )


@dataclass
class ReasonedPrediction:
    """A prediction with associated reasoning."""

    prediction_value: float | dict[str, Any]
    reasoning: str
    confidence: str = "medium"  # low, medium, high
    cost_usd: float = 0.0
    model_used: str = ""


@dataclass
class ForecastResult:
    """Result of an LLM forecast."""

    question: BottleneckQuestion
    prediction: ReasonedPrediction
    research_context: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "question_id": self.question.id,
            "question_type": self.question.question_type,
            "prediction": self.prediction.prediction_value,
            "reasoning": self.prediction.reasoning,
            "confidence": self.prediction.confidence,
            "cost_usd": self.prediction.cost_usd,
            "model_used": self.prediction.model_used,
            "research_context_length": len(self.research_context),
            "created_at": self.created_at.isoformat(),
            "errors": self.errors,
        }


class BottleneckForecastBot:
    """LLM-powered forecaster for bottleneck questions.

    This class adapts the Metaculus forecasting patterns to work with
    our bottleneck data models and research collectors.
    """

    def __init__(
        self,
        default_model: str = "gpt-5",
        research_model: str = "claude-3-5-sonnet-20241022",
    ):
        """Initialize the forecast bot.

        Args:
            default_model: Default model for forecasting
            research_model: Model for research synthesis
        """
        self.default_model = default_model
        self.research_model = research_model
        self.logger = logger.bind(component="bottleneck_forecast_bot")

        # Track costs
        self._total_cost_usd = 0.0
        self._forecast_count = 0

    async def gather_research_context(
        self,
        bottleneck: BottleneckSignalData,
    ) -> str:
        """Gather research context using existing research collectors.

        Args:
            bottleneck: The bottleneck to research

        Returns:
            Formatted research context string
        """
        try:
            from src.data_ingestion.research.aggregator import ResearchAggregator

            topics = self._bottleneck_to_research_topics(bottleneck)

            aggregator = ResearchAggregator()
            papers = await aggregator.collect_recent(days=30, topics=topics)

            if not papers:
                self.logger.info(
                    "No research papers found",
                    category=bottleneck.category.value,
                    topics=topics,
                )
                return ""

            return self._format_papers_for_llm(papers[:10])

        except ImportError:
            self.logger.warning("Research aggregator not available")
            return ""
        except Exception as e:
            self.logger.error("Failed to gather research", error=str(e))
            return ""

    def _bottleneck_to_research_topics(
        self,
        bottleneck: BottleneckSignalData,
    ) -> list[str]:
        """Map bottleneck category to research topics.

        Args:
            bottleneck: The bottleneck signal

        Returns:
            List of topic strings for research
        """
        category_topics: dict[str, list[str]] = {
            "inventory_squeeze": ["supply_chain", "materials"],
            "price_spike": ["macro_geoeconomics", "energy"],
            "shipping_congestion": ["supply_chain", "transportation"],
            "labor_tightness": ["supply_chain", "manufacturing"],
            "capacity_ceiling": ["manufacturing", "energy"],
            "demand_surge": ["consumer", "macro_geoeconomics"],
            "supply_disruption": ["supply_chain", "semiconductors"],
            "energy_crunch": ["energy", "macro_geoeconomics"],
            "credit_tightening": ["fintech_digital_assets", "macro_geoeconomics"],
            "sentiment_shift": ["consumer", "macro_geoeconomics"],
        }

        return category_topics.get(bottleneck.category.value, ["supply_chain"])

    def _format_papers_for_llm(self, papers: list[Any]) -> str:
        """Format research papers for LLM context.

        Args:
            papers: List of research paper objects

        Returns:
            Formatted string for LLM prompt
        """
        if not papers:
            return "No recent research available."

        formatted_parts = []

        for i, paper in enumerate(papers, 1):
            title = getattr(paper, "title", "Unknown Title")
            abstract = getattr(paper, "abstract", "")
            published = getattr(paper, "published_date", "")
            source = getattr(paper, "source", "")

            formatted_parts.append(
                f"### Paper {i}: {title}\n"
                f"- **Source**: {source}\n"
                f"- **Published**: {published}\n"
                f"- **Abstract**: {abstract[:500]}..."
                if len(abstract) > 500
                else f"- **Abstract**: {abstract}"
            )

        return "\n\n".join(formatted_parts)

    def _get_historical_precedents(
        self,
        bottleneck: BottleneckSignalData,
    ) -> str:
        """Get historical precedents for this type of bottleneck.

        Args:
            bottleneck: The bottleneck signal

        Returns:
            Formatted precedents string
        """
        precedent_database: dict[str, list[dict[str, Any]]] = {
            "supply_disruption": [
                {
                    "event": "2021 Semiconductor Shortage",
                    "duration_days": 365,
                    "peak_severity": 0.85,
                    "resolution": "Gradual capacity expansion",
                },
                {
                    "event": "2020 COVID Supply Chain Disruption",
                    "duration_days": 180,
                    "peak_severity": 0.90,
                    "resolution": "Demand normalization and logistics adaptation",
                },
            ],
            "energy_crunch": [
                {
                    "event": "2022 European Energy Crisis",
                    "duration_days": 270,
                    "peak_severity": 0.80,
                    "resolution": "Demand reduction and alternative sourcing",
                },
                {
                    "event": "2008 Oil Price Spike",
                    "duration_days": 150,
                    "peak_severity": 0.75,
                    "resolution": "Demand destruction and financial crisis",
                },
            ],
            "shipping_congestion": [
                {
                    "event": "2021 Port Congestion",
                    "duration_days": 120,
                    "peak_severity": 0.75,
                    "resolution": "Extended operating hours and demand normalization",
                },
            ],
            "inventory_squeeze": [
                {
                    "event": "2021 Retail Inventory Crisis",
                    "duration_days": 90,
                    "peak_severity": 0.65,
                    "resolution": "Supply chain adaptation and inventory rebuilding",
                },
            ],
            "labor_tightness": [
                {
                    "event": "2022 Great Resignation Impact",
                    "duration_days": 180,
                    "peak_severity": 0.70,
                    "resolution": "Wage increases and hiring adjustments",
                },
            ],
        }

        precedents = precedent_database.get(bottleneck.category.value, [])

        if not precedents:
            return "No historical precedents available for this category."

        formatted_parts = []
        for p in precedents:
            formatted_parts.append(
                f"- **{p['event']}**: Duration {p['duration_days']} days, "
                f"Peak severity {p['peak_severity']:.0%}, "
                f"Resolution: {p['resolution']}"
            )

        return "\n".join(formatted_parts)

    async def forecast_duration(
        self,
        bottleneck: BottleneckSignalData,
        research_context: str | None = None,
    ) -> ForecastResult:
        """Forecast how long a bottleneck will persist.

        Args:
            bottleneck: The bottleneck to forecast
            research_context: Optional pre-gathered research context

        Returns:
            ForecastResult with duration prediction
        """
        if not is_llm_available():
            raise RuntimeError("LLM not available for forecasting")

        question = BottleneckQuestion.from_bottleneck(
            bottleneck, question_type="duration"
        )

        if research_context is None:
            research_context = await self.gather_research_context(bottleneck)

        precedents = self._get_historical_precedents(bottleneck)

        prompt = format_duration_prompt(
            category=bottleneck.category.value,
            severity=bottleneck.severity,
            confidence=bottleneck.confidence,
            sectors=bottleneck.affected_sectors,
            description=bottleneck.description,
            detected_at=bottleneck.detected_at.isoformat(),
            source_series=bottleneck.source_series,
            research_context=research_context,
            precedents=precedents,
        )

        llm = get_forecasting_llm("default")
        response = await llm.invoke(prompt)

        prediction = self._parse_duration_response(response, bottleneck)
        prediction.model_used = self.default_model

        input_tokens = len(prompt.split()) * 1.3
        output_tokens = len(response.split()) * 1.3
        prediction.cost_usd = estimate_cost(
            self.default_model, int(input_tokens), int(output_tokens)
        )

        self._total_cost_usd += prediction.cost_usd
        self._forecast_count += 1

        self.logger.info(
            "Generated duration forecast",
            bottleneck_id=str(bottleneck.id),
            expected_days=prediction.prediction_value.get("expected_duration_days"),
            cost_usd=prediction.cost_usd,
        )

        return ForecastResult(
            question=question,
            prediction=prediction,
            research_context=research_context,
        )

    def _parse_duration_response(
        self,
        response: str,
        bottleneck: BottleneckSignalData,
    ) -> ReasonedPrediction:
        """Parse LLM duration forecast response.

        Args:
            response: Raw LLM response
            bottleneck: Original bottleneck

        Returns:
            Parsed ReasonedPrediction
        """
        prediction: dict[str, Any] = {}

        patterns = {
            "expected_duration_days": r"EXPECTED_DURATION_DAYS:\s*(\d+)",
            "percentile_25": r"PERCENTILE_25:\s*(\d+)",
            "percentile_75": r"PERCENTILE_75:\s*(\d+)",
            "probability_persists_30_days": r"PROBABILITY_PERSISTS_30_DAYS:\s*([\d.]+)",
            "probability_persists_60_days": r"PROBABILITY_PERSISTS_60_DAYS:\s*([\d.]+)",
            "probability_persists_90_days": r"PROBABILITY_PERSISTS_90_DAYS:\s*([\d.]+)",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, response)
            if match:
                value = match.group(1)
                if "probability" in key:
                    prediction[key] = float(value)
                else:
                    prediction[key] = int(value)

        reasoning_match = re.search(r"REASONING:\s*(.+)", response, re.DOTALL)
        reasoning = reasoning_match.group(1).strip() if reasoning_match else response

        if "expected_duration_days" not in prediction:
            # Fallback to category-based default
            category_defaults = {
                "inventory_squeeze": 45,
                "price_spike": 30,
                "energy_crunch": 50,
                "supply_disruption": 75,
                "shipping_congestion": 60,
                "labor_tightness": 90,
            }
            prediction["expected_duration_days"] = category_defaults.get(
                bottleneck.category.value, 60
            )
            prediction["percentile_25"] = int(prediction["expected_duration_days"] * 0.6)
            prediction["percentile_75"] = int(prediction["expected_duration_days"] * 1.4)

        return ReasonedPrediction(
            prediction_value=prediction,
            reasoning=reasoning,
            confidence="medium",
        )

    async def forecast_trajectory(
        self,
        bottleneck: BottleneckSignalData,
        horizon_days: int = 30,
        research_context: str | None = None,
    ) -> ForecastResult:
        """Forecast severity trajectory over time.

        Args:
            bottleneck: The bottleneck to forecast
            horizon_days: Forecast horizon in days
            research_context: Optional pre-gathered research

        Returns:
            ForecastResult with trajectory prediction
        """
        if not is_llm_available():
            raise RuntimeError("LLM not available for forecasting")

        question = BottleneckQuestion.from_bottleneck(
            bottleneck, question_type="trajectory"
        )

        if research_context is None:
            research_context = await self.gather_research_context(bottleneck)

        age_days = (datetime.now(UTC) - bottleneck.detected_at).days

        prompt = format_trajectory_prompt(
            category=bottleneck.category.value,
            severity=bottleneck.severity,
            confidence=bottleneck.confidence,
            sectors=bottleneck.affected_sectors,
            description=bottleneck.description,
            age_days=age_days,
            horizon_days=horizon_days,
            research_context=research_context,
        )

        llm = get_forecasting_llm("default")
        response = await llm.invoke(prompt)

        prediction = self._parse_trajectory_response(response)
        prediction.model_used = self.default_model

        input_tokens = len(prompt.split()) * 1.3
        output_tokens = len(response.split()) * 1.3
        prediction.cost_usd = estimate_cost(
            self.default_model, int(input_tokens), int(output_tokens)
        )

        self._total_cost_usd += prediction.cost_usd
        self._forecast_count += 1

        return ForecastResult(
            question=question,
            prediction=prediction,
            research_context=research_context,
        )

    def _parse_trajectory_response(self, response: str) -> ReasonedPrediction:
        """Parse trajectory forecast response."""
        prediction: dict[str, Any] = {}

        patterns = {
            "day_7_severity": r"DAY_7_SEVERITY:\s*([\d.]+)",
            "day_14_severity": r"DAY_14_SEVERITY:\s*([\d.]+)",
            "day_21_severity": r"DAY_21_SEVERITY:\s*([\d.]+)",
            "day_30_severity": r"DAY_30_SEVERITY:\s*([\d.]+)",
            "expected_resolution_day": r"EXPECTED_RESOLUTION_DAY:\s*(\S+)",
            "trajectory_type": r"TRAJECTORY_TYPE:\s*(\w+)",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, response)
            if match:
                value = match.group(1)
                if "severity" in key:
                    prediction[key] = float(value)
                elif key == "expected_resolution_day":
                    prediction[key] = int(value) if value.isdigit() else None
                else:
                    prediction[key] = value

        reasoning_match = re.search(r"REASONING:\s*(.+)", response, re.DOTALL)
        reasoning = reasoning_match.group(1).strip() if reasoning_match else response

        return ReasonedPrediction(
            prediction_value=prediction,
            reasoning=reasoning,
            confidence="medium",
        )

    @property
    def total_cost(self) -> float:
        """Total cost of all forecasts in USD."""
        return self._total_cost_usd

    @property
    def forecast_count(self) -> int:
        """Number of forecasts made."""
        return self._forecast_count


# Global forecast bot instance
_forecast_bot: BottleneckForecastBot | None = None


def get_forecast_bot() -> BottleneckForecastBot:
    """Get the global forecast bot instance."""
    global _forecast_bot
    if _forecast_bot is None:
        _forecast_bot = BottleneckForecastBot()
    return _forecast_bot
