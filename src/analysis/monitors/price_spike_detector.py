"""Price Spike Detector for commodity price breakout identification.

Monitors commodity prices across groups:
- Energy: WTI Oil, Natural Gas, Gasoline
- Metals: Copper, Aluminum, Nickel, Zinc
- Agriculture: Corn, Soybeans, Wheat

Detection methods:
- Percentage change threshold (default: >10%)
- Z-score significance testing
- Multi-timeframe analysis
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from typing import Any

import numpy as np
import pandas as pd
import structlog

from src.analysis.monitors.base_monitor import BaseMonitor, MonitorResult
from src.analysis.signals import BottleneckCategory, BottleneckSignalData
from src.storage.timescale import TimescaleDB

logger = structlog.get_logger()


@dataclass
class PriceBreakout:
    """Represents a detected price breakout."""

    commodity: str
    commodity_group: str
    current_price: float
    previous_price: float
    pct_change: float
    z_score: float
    timeframe: str  # "daily", "weekly", "monthly"
    direction: str  # "up", "down"
    is_significant: bool
    score: float  # 0-100 breakout intensity
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "commodity": self.commodity,
            "commodity_group": self.commodity_group,
            "current_price": self.current_price,
            "previous_price": self.previous_price,
            "pct_change": self.pct_change,
            "z_score": self.z_score,
            "timeframe": self.timeframe,
            "direction": self.direction,
            "is_significant": self.is_significant,
            "score": self.score,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


# Commodity configurations
COMMODITY_GROUPS = {
    "energy": {
        "commodities": {
            "wti_oil": {"series_id": "DCOILWTICO", "name": "WTI Crude Oil", "unit": "USD/barrel"},
            "natural_gas": {"series_id": "DHHNGSP", "name": "Henry Hub Natural Gas", "unit": "USD/MMBtu"},
            "gasoline": {"series_id": "GASREGW", "name": "Regular Gasoline", "unit": "USD/gallon"},
        },
        "weight": 0.40,
        "pct_threshold": 0.10,
    },
    "metals": {
        "commodities": {
            "copper": {"series_id": "PCOPPUSDM", "name": "Copper", "unit": "USD/MT"},
            "aluminum": {"series_id": "PALUMUSDM", "name": "Aluminum", "unit": "USD/MT"},
            "nickel": {"series_id": "PNICKUSDM", "name": "Nickel", "unit": "USD/MT"},
            "zinc": {"series_id": "PZINCUSDM", "name": "Zinc", "unit": "USD/MT"},
        },
        "weight": 0.35,
        "pct_threshold": 0.08,
    },
    "agriculture": {
        "commodities": {
            "corn": {"series_id": "PMAIZMTUSDM", "name": "Corn", "unit": "USD/MT"},
            "soybeans": {"series_id": "PSOYBUSDM", "name": "Soybeans", "unit": "USD/MT"},
            "wheat": {"series_id": "PWHEAMTUSDM", "name": "Wheat", "unit": "USD/MT"},
        },
        "weight": 0.25,
        "pct_threshold": 0.12,
    },
}


class PriceSpikeDetector(BaseMonitor):
    """Detector for commodity price spikes and breakouts.

    Identifies significant price movements using:
    - Percentage change thresholds
    - Z-score statistical significance
    - Multi-timeframe analysis (daily, weekly, monthly)
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        pct_threshold: float = 0.10,
        z_score_threshold: float = 2.5,
        lookback_days: int = 365,
        **kwargs,
    ):
        """Initialize the detector.

        Args:
            db: Database connection
            pct_threshold: Default percentage change threshold
            z_score_threshold: Z-score threshold for significance
            lookback_days: Days of history for baseline
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(db=db, **kwargs)
        self.pct_threshold = pct_threshold
        self.z_score_threshold = z_score_threshold
        self.lookback_days = lookback_days
        self.commodity_groups = COMMODITY_GROUPS.copy()

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.PRICE_SPIKE

    async def detect_breakouts(
        self,
        pct_threshold: float | None = None,
        z_score_threshold: float | None = None,
    ) -> list[PriceBreakout]:
        """Detect price breakouts across all commodities.

        Args:
            pct_threshold: Override percentage threshold
            z_score_threshold: Override Z-score threshold

        Returns:
            List of detected price breakouts
        """
        if pct_threshold is None:
            pct_threshold = self.pct_threshold
        if z_score_threshold is None:
            z_score_threshold = self.z_score_threshold

        breakouts = []

        for group_name, group_config in self.commodity_groups.items():
            group_pct_threshold = group_config.get("pct_threshold", pct_threshold)

            for commodity_name, commodity_config in group_config["commodities"].items():
                series = await self.fetch_series(
                    commodity_config["series_id"],
                    lookback_days=self.lookback_days,
                )

                if series is None or len(series) < 30:
                    continue

                for timeframe in ["daily", "weekly", "monthly"]:
                    breakout = self._check_breakout(
                        commodity_name=commodity_name,
                        commodity_group=group_name,
                        series=series,
                        timeframe=timeframe,
                        pct_threshold=group_pct_threshold,
                        z_score_threshold=z_score_threshold,
                        commodity_config=commodity_config,
                    )

                    if breakout is not None:
                        breakouts.append(breakout)

        breakouts.sort(key=lambda b: b.score, reverse=True)

        return breakouts

    def _check_breakout(
        self,
        commodity_name: str,
        commodity_group: str,
        series: pd.Series,
        timeframe: str,
        pct_threshold: float,
        z_score_threshold: float,
        commodity_config: dict[str, Any],
    ) -> PriceBreakout | None:
        """Check for breakout in a specific timeframe.

        Args:
            commodity_name: Name of the commodity
            commodity_group: Group name (energy, metals, agriculture)
            series: Price time series
            timeframe: Timeframe to analyze
            pct_threshold: Percentage threshold
            z_score_threshold: Z-score threshold
            commodity_config: Commodity configuration

        Returns:
            PriceBreakout if detected, None otherwise
        """
        if timeframe == "daily":
            lookback = 1
            window = 30
        elif timeframe == "weekly":
            lookback = 5
            window = 52
        else:  # monthly
            lookback = 21
            window = 12

        if len(series) <= lookback:
            return None

        current_price = float(series.iloc[-1])
        previous_price = float(series.iloc[-lookback - 1])

        if previous_price == 0:
            return None

        pct_change = (current_price - previous_price) / previous_price

        returns = series.pct_change().dropna()
        if len(returns) < window:
            return None

        recent_returns = returns.tail(window)
        mean_return = float(recent_returns.mean())
        std_return = float(recent_returns.std())

        if std_return == 0:
            z_score = 0.0
        else:
            # Z-score of the current period's return
            current_return = pct_change
            z_score = (current_return - mean_return) / std_return

        is_pct_significant = abs(pct_change) >= pct_threshold
        is_z_significant = abs(z_score) >= z_score_threshold
        is_significant = is_pct_significant or is_z_significant

        if not is_significant:
            return None

        pct_score = min(100.0, abs(pct_change) / (pct_threshold * 2) * 100)
        z_score_normalized = min(100.0, abs(z_score) / 4.0 * 100)
        score = max(pct_score, z_score_normalized)

        direction = "up" if pct_change > 0 else "down"

        return PriceBreakout(
            commodity=commodity_name,
            commodity_group=commodity_group,
            current_price=current_price,
            previous_price=previous_price,
            pct_change=pct_change,
            z_score=z_score,
            timeframe=timeframe,
            direction=direction,
            is_significant=is_significant,
            score=score,
            metadata={
                "series_id": commodity_config["series_id"],
                "unit": commodity_config["unit"],
                "pct_threshold": pct_threshold,
                "z_threshold": z_score_threshold,
            },
        )

    async def calculate_significance(
        self,
        series_id: str,
        window: int = 90,
    ) -> dict[str, float]:
        """Calculate statistical significance metrics for a series.

        Args:
            series_id: Series identifier
            window: Rolling window for calculations

        Returns:
            Dictionary with significance metrics
        """
        series = await self.fetch_series(series_id, lookback_days=window * 2)

        if series is None or len(series) < window:
            return {}

        returns = series.pct_change().dropna()

        z_score, mean, std = self.get_latest_z_score(returns, window=window)

        return {
            "current_return": float(returns.iloc[-1]) if len(returns) > 0 else 0.0,
            "mean_return": mean,
            "std_return": std,
            "z_score": z_score,
            "is_significant": abs(z_score) >= self.z_score_threshold,
        }

    async def get_group_summary(
        self,
        group_name: str,
    ) -> dict[str, Any]:
        """Get summary for a commodity group.

        Args:
            group_name: Name of the group (energy, metals, agriculture)

        Returns:
            Summary dictionary
        """
        if group_name not in self.commodity_groups:
            return {}

        group = self.commodity_groups[group_name]
        breakouts = []

        for commodity_name, commodity_config in group["commodities"].items():
            series = await self.fetch_series(
                commodity_config["series_id"],
                lookback_days=self.lookback_days,
            )

            if series is None or len(series) < 2:
                continue

            current = float(series.iloc[-1])
            previous = float(series.iloc[-2])
            pct_change = (current - previous) / previous if previous != 0 else 0

            breakouts.append({
                "commodity": commodity_name,
                "current_price": current,
                "pct_change_1d": pct_change,
                "unit": commodity_config["unit"],
            })

        return {
            "group": group_name,
            "commodities": breakouts,
            "weight": group["weight"],
            "pct_threshold": group["pct_threshold"],
        }

    async def calculate_score(self) -> MonitorResult:
        """Calculate composite price spike score.

        Returns:
            MonitorResult with composite score
        """
        breakouts = await self.detect_breakouts()

        if not breakouts:
            return MonitorResult(
                score=0.0,
                severity=0.0,
                confidence=0.5,
                description="No significant price spikes detected",
            )

        group_scores: dict[str, list[float]] = {
            group: [] for group in self.commodity_groups
        }

        for breakout in breakouts:
            group_scores[breakout.commodity_group].append(breakout.score)

        components = {}
        weights = {}

        for group_name, scores in group_scores.items():
            if scores:
                components[group_name] = max(scores)
            else:
                components[group_name] = 0.0
            weights[group_name] = self.commodity_groups[group_name]["weight"]

        composite_score = self.weighted_composite(components, weights)

        upward_spikes = sum(1 for b in breakouts if b.direction == "up")
        downward_spikes = sum(1 for b in breakouts if b.direction == "down")

        if upward_spikes > downward_spikes:
            direction = "upward"
        elif downward_spikes > upward_spikes:
            direction = "downward"
        else:
            direction = "mixed"

        description = (
            f"Detected {len(breakouts)} price breakouts ({direction} bias): "
            f"{upward_spikes} up, {downward_spikes} down"
        )

        total_commodities = sum(
            len(g["commodities"]) for g in self.commodity_groups.values()
        )
        data_completeness = len(
            set(b.commodity for b in breakouts)
        ) / total_commodities if total_commodities > 0 else 0

        confidence = self.calculate_confidence(
            data_completeness=max(0.5, data_completeness),
            sample_size=len(breakouts) * 30,
            min_samples=30,
        )

        return MonitorResult(
            score=composite_score,
            severity=self.score_to_severity(composite_score),
            confidence=confidence,
            components=components,
            evidence={
                "breakouts": [b.to_dict() for b in breakouts[:10]],  # Top 10
                "total_breakouts": len(breakouts),
                "upward_spikes": upward_spikes,
                "downward_spikes": downward_spikes,
                "direction_bias": direction,
            },
            description=description,
        )

    async def generate_signals(self) -> list[BottleneckSignalData]:
        """Generate bottleneck signals for detected price spikes.

        Returns:
            List of BottleneckSignalData for significant spikes
        """
        breakouts = await self.detect_breakouts()
        signals = []

        for breakout in breakouts:
            if breakout.score < self.alert_threshold:
                continue

            signal = BottleneckSignalData(
                category=BottleneckCategory.PRICE_SPIKE,
                subcategory=f"{breakout.commodity_group}_price_spike",
                severity=min(1.0, breakout.score / 100),
                confidence=0.75 if breakout.is_significant else 0.6,
                affected_sectors=self._get_affected_sectors(breakout.commodity_group),
                affected_commodities=[breakout.commodity],
                source_series=[breakout.metadata.get("series_id", "")],
                evidence={
                    "current_price": breakout.current_price,
                    "previous_price": breakout.previous_price,
                    "pct_change": breakout.pct_change,
                    "z_score": breakout.z_score,
                    "timeframe": breakout.timeframe,
                    "direction": breakout.direction,
                },
                description=(
                    f"Price spike in {breakout.commodity}: "
                    f"{breakout.pct_change:+.1%} ({breakout.timeframe}), "
                    f"Z-score: {breakout.z_score:.2f}"
                ),
            )
            signals.append(signal)

        return signals

    @staticmethod
    def _get_affected_sectors(commodity_group: str) -> list[str]:
        """Get affected sectors for a commodity group."""
        sector_map = {
            "energy": ["ENERGY", "TRANSPORTATION", "MANUFACTURING", "UTILITIES"],
            "metals": ["MANUFACTURING", "CONSTRUCTION", "MINING"],
            "agriculture": ["AGRICULTURE", "FOOD", "CONSUMER"],
        }
        return sector_map.get(commodity_group, ["CONSUMER"])
