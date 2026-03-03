"""Inventory-to-Sales Ratio Monitor for detecting inventory squeezes.

Monitors inventory-to-sales ratios across sectors:
- Retail (RETAILIRSA)
- Wholesale (WHLSLRIMSA)
- Manufacturing (MNFCTRIRSA)

Detects:
- Inventory squeezes (ratio drops below 2σ threshold)
- Demand weakness (ratio rises above 2σ threshold)
- Cross-sector stress patterns
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
class ISRatioResult:
    """Result for a single sector's I/S ratio analysis."""

    sector: str
    series_id: str
    current_ratio: float
    mean_ratio: float
    std_ratio: float
    z_score: float
    percentile: float  # 0-100 where 0 = lowest historical
    baseline_deviation_pct: float
    status: str  # "squeeze", "normal", "elevated"
    score: float  # 0-100 stress score
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sector": self.sector,
            "series_id": self.series_id,
            "current_ratio": self.current_ratio,
            "mean_ratio": self.mean_ratio,
            "std_ratio": self.std_ratio,
            "z_score": self.z_score,
            "percentile": self.percentile,
            "baseline_deviation_pct": self.baseline_deviation_pct,
            "status": self.status,
            "score": self.score,
            "timestamp": self.timestamp.isoformat(),
        }


# Sector configurations
SECTOR_CONFIG = {
    "retail": {
        "series_id": "RETAILIRSA",
        "name": "Retail Inventory/Sales Ratio",
        "baseline": 1.40,
        "squeeze_threshold": 1.20,  # Below this is concerning
        "elevated_threshold": 1.60,  # Above this suggests weak demand
        "weight": 0.35,
    },
    "wholesale": {
        "series_id": "WHLSLRIMSA",
        "name": "Wholesale Inventory/Sales Ratio",
        "baseline": 1.25,
        "squeeze_threshold": 1.10,
        "elevated_threshold": 1.45,
        "weight": 0.35,
    },
    "manufacturing": {
        "series_id": "MNFCTRIRSA",
        "name": "Manufacturing Inventory/Sales Ratio",
        "baseline": 1.35,
        "squeeze_threshold": 1.20,
        "elevated_threshold": 1.55,
        "weight": 0.30,
    },
}


class InventorySalesMonitor(BaseMonitor):
    """Monitor for inventory-to-sales ratios across sectors.

    Calculates I/S ratios for retail, wholesale, and manufacturing sectors,
    comparing against historical baselines and generating alerts when
    ratios indicate inventory stress.
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        sigma_threshold: float = 2.0,
        lookback_days: int = 365,
        **kwargs,
    ):
        """Initialize the monitor.

        Args:
            db: Database connection
            sigma_threshold: Number of standard deviations for alert
            lookback_days: Days of history for baseline calculation
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(db=db, **kwargs)
        self.sigma_threshold = sigma_threshold
        self.lookback_days = lookback_days
        self.sectors = SECTOR_CONFIG.copy()

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.INVENTORY_SQUEEZE

    async def calculate_ratios(self) -> dict[str, ISRatioResult]:
        """Calculate I/S ratios for all sectors.

        Returns:
            Dictionary mapping sector name to ISRatioResult
        """
        results = {}
        series_ids = [cfg["series_id"] for cfg in self.sectors.values()]

        series_data = await self.fetch_multiple_series(
            series_ids, lookback_days=self.lookback_days
        )

        for sector, config in self.sectors.items():
            series_id = config["series_id"]
            series = series_data.get(series_id)

            if series is None or len(series) < 10:
                self.logger.warning(
                    "Insufficient data for sector",
                    sector=sector,
                    series_id=series_id,
                )
                continue

            result = self._analyze_sector(sector, config, series)
            results[sector] = result

        return results

    def _analyze_sector(
        self,
        sector: str,
        config: dict[str, Any],
        series: pd.Series,
    ) -> ISRatioResult:
        """Analyze a single sector's I/S ratio.

        Args:
            sector: Sector name
            config: Sector configuration
            series: Time series of I/S ratios

        Returns:
            ISRatioResult with analysis
        """
        current_ratio = float(series.iloc[-1])

        historical = series.iloc[:-1] if len(series) > 1 else series
        mean_ratio = float(historical.mean())
        std_ratio = float(historical.std())

        z_score = self.calculate_z_score(current_ratio, mean_ratio, std_ratio)

        # Percentile (lower = more squeeze risk)
        percentile = self.normalize_percentile_to_100(current_ratio, historical)

        baseline = config["baseline"]
        _, baseline_deviation_pct = self.calculate_deviation_from_baseline(
            current_ratio, baseline
        )

        squeeze_threshold = config["squeeze_threshold"]
        elevated_threshold = config["elevated_threshold"]

        if current_ratio < squeeze_threshold or z_score < -self.sigma_threshold:
            status = "squeeze"
        elif current_ratio > elevated_threshold or z_score > self.sigma_threshold:
            status = "elevated"
        else:
            status = "normal"

        # For inventory squeeze: low ratio = high stress
        # Invert the percentile for squeeze detection
        if status == "squeeze":
            # Low percentile = high squeeze risk
            score = 100 - percentile
        elif status == "elevated":
            # High percentile = elevated inventories (less urgent but notable)
            score = percentile * 0.5  # Lower weight for elevated
        else:
            score = 25.0  # Normal baseline

        if abs(z_score) >= self.sigma_threshold:
            z_boost = min(25.0, abs(z_score) * 5)
            score = min(100.0, score + z_boost)

        return ISRatioResult(
            sector=sector,
            series_id=config["series_id"],
            current_ratio=current_ratio,
            mean_ratio=mean_ratio,
            std_ratio=std_ratio,
            z_score=z_score,
            percentile=percentile,
            baseline_deviation_pct=baseline_deviation_pct,
            status=status,
            score=score,
        )

    async def check_thresholds(
        self,
        sigma: float | None = None,
    ) -> list[BottleneckSignalData]:
        """Check all sectors and generate signals for threshold breaches.

        Args:
            sigma: Override sigma threshold

        Returns:
            List of BottleneckSignalData for threshold breaches
        """
        if sigma is None:
            sigma = self.sigma_threshold

        ratio_results = await self.calculate_ratios()
        signals = []

        for sector, result in ratio_results.items():
            if result.status == "squeeze":
                signal = BottleneckSignalData(
                    category=BottleneckCategory.INVENTORY_SQUEEZE,
                    subcategory=f"{sector}_inventory_squeeze",
                    severity=min(1.0, result.score / 100),
                    confidence=0.8 if abs(result.z_score) >= sigma else 0.65,
                    affected_sectors=[sector.upper()],
                    source_series=[result.series_id],
                    evidence={
                        "current_ratio": result.current_ratio,
                        "mean_ratio": result.mean_ratio,
                        "z_score": result.z_score,
                        "percentile": result.percentile,
                        "baseline_deviation_pct": result.baseline_deviation_pct,
                        "score": result.score,
                    },
                    description=(
                        f"{sector.capitalize()} inventory squeeze detected: "
                        f"I/S ratio at {result.current_ratio:.2f} "
                        f"(Z-score: {result.z_score:.2f}, "
                        f"{result.percentile:.0f}th percentile)"
                    ),
                )
                signals.append(signal)

        return signals

    async def get_historical_baseline(
        self,
        sector: str,
        window_days: int = 365,
    ) -> dict[str, float]:
        """Get historical baseline statistics for a sector.

        Args:
            sector: Sector name
            window_days: Number of days for baseline

        Returns:
            Dictionary with baseline statistics
        """
        if sector not in self.sectors:
            return {}

        config = self.sectors[sector]
        series = await self.fetch_series(config["series_id"], lookback_days=window_days)

        if series is None or len(series) < 10:
            return {}

        return {
            "mean": float(series.mean()),
            "std": float(series.std()),
            "min": float(series.min()),
            "max": float(series.max()),
            "p25": float(series.quantile(0.25)),
            "p50": float(series.quantile(0.50)),
            "p75": float(series.quantile(0.75)),
            "current": float(series.iloc[-1]),
            "configured_baseline": config["baseline"],
        }

    async def calculate_score(self) -> MonitorResult:
        """Calculate composite inventory stress score.

        Returns:
            MonitorResult with composite score and component breakdown
        """
        ratio_results = await self.calculate_ratios()

        if not ratio_results:
            return MonitorResult(
                score=0.0,
                severity=0.0,
                confidence=0.0,
                description="No data available for inventory analysis",
            )

        components = {}
        weights = {}

        for sector, result in ratio_results.items():
            components[sector] = result.score
            weights[sector] = self.sectors[sector]["weight"]

        composite_score = self.weighted_composite(components, weights)

        squeeze_count = sum(1 for r in ratio_results.values() if r.status == "squeeze")
        if squeeze_count >= 2:
            description = f"Multi-sector inventory squeeze: {squeeze_count} sectors affected"
        elif squeeze_count == 1:
            squeeze_sector = next(
                s for s, r in ratio_results.items() if r.status == "squeeze"
            )
            description = f"Inventory squeeze in {squeeze_sector} sector"
        else:
            description = "Inventory levels within normal ranges"

        data_completeness = len(ratio_results) / len(self.sectors)
        confidence = self.calculate_confidence(
            data_completeness=data_completeness,
            sample_size=len(ratio_results) * 12,  # Assume monthly data
            min_samples=24,
        )

        return MonitorResult(
            score=composite_score,
            severity=self.score_to_severity(composite_score),
            confidence=confidence,
            components=components,
            evidence={
                "sector_results": {
                    sector: result.to_dict()
                    for sector, result in ratio_results.items()
                },
                "squeeze_count": squeeze_count,
                "sectors_analyzed": len(ratio_results),
            },
            description=description,
        )

    async def get_sector_summary(self) -> dict[str, Any]:
        """Get a summary of all sector I/S ratios.

        Returns:
            Dictionary with sector summaries
        """
        ratio_results = await self.calculate_ratios()
        result = await self.calculate_score()

        return {
            "composite_score": result.score,
            "alert_level": result.alert_level,
            "sectors": {
                sector: {
                    "current_ratio": r.current_ratio,
                    "status": r.status,
                    "z_score": r.z_score,
                    "score": r.score,
                }
                for sector, r in ratio_results.items()
            },
            "timestamp": datetime.now(UTC).isoformat(),
        }
