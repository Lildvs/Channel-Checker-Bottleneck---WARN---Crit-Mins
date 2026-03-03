"""Capacity Utilization Monitor for detecting capacity constraints.

Monitors industrial capacity utilization by sector:
- Total Industry (TCU)
- Manufacturing (MCUMFN)
- Mining (CAPUTLG211S)
- Utilities (CAPUTLG2211S)

Data Sources (Federal Reserve via FRED):
- TCU: Capacity Utilization: Total Industry
- MCUMFN: Capacity Utilization: Manufacturing
- CAPUTLG211S: Capacity Utilization: Mining
- CAPUTLG2211S: Capacity Utilization: Utilities
- INDPRO: Industrial Production Index
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
class SectorUtilization:
    """Capacity utilization for a specific sector."""

    sector: str
    sector_name: str
    current_utilization: float  # As decimal (0.85 = 85%)
    threshold: float  # Alert threshold
    historical_avg: float
    deviation_from_avg: float  # Percentage points
    z_score: float
    is_above_threshold: bool
    score: float  # 0-100 stress score
    status: str  # "normal", "elevated", "critical"
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sector": self.sector,
            "sector_name": self.sector_name,
            "current_utilization": self.current_utilization,
            "threshold": self.threshold,
            "historical_avg": self.historical_avg,
            "deviation_from_avg": self.deviation_from_avg,
            "z_score": self.z_score,
            "is_above_threshold": self.is_above_threshold,
            "score": self.score,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
        }


# Sector configurations
SECTOR_CONFIG = {
    "total": {
        "series_id": "TCU",
        "name": "Total Industry",
        "threshold": 0.85,  # 85%
        "critical_threshold": 0.90,
        "weight": 0.40,
    },
    "manufacturing": {
        "series_id": "MCUMFN",
        "name": "Manufacturing",
        "threshold": 0.80,  # Manufacturing tends to run tighter
        "critical_threshold": 0.85,
        "weight": 0.30,
    },
    "mining": {
        "series_id": "CAPUTLG211S",
        "name": "Mining",
        "threshold": 0.90,  # Mining can run higher
        "critical_threshold": 0.95,
        "weight": 0.15,
    },
    "utilities": {
        "series_id": "CAPUTLG2211S",
        "name": "Utilities",
        "threshold": 0.85,
        "critical_threshold": 0.90,
        "weight": 0.15,
    },
}

# Additional series for context
CONTEXT_SERIES = {
    "industrial_production": "INDPRO",  # Industrial Production Index
}


class CapacityUtilizationMonitor(BaseMonitor):
    """Monitor for industrial capacity utilization.

    Tracks capacity utilization across sectors and generates alerts
    when utilization exceeds thresholds indicating capacity constraints.
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        lookback_days: int = 365 * 5,  # 5 years for historical comparison
        **kwargs,
    ):
        """Initialize the monitor.

        Args:
            db: Database connection
            lookback_days: Days of history for baseline
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(db=db, **kwargs)
        self.lookback_days = lookback_days
        self.sectors = SECTOR_CONFIG.copy()

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.CAPACITY_CEILING

    async def get_sector_utilization(self, sector: str) -> SectorUtilization | None:
        """Get utilization data for a specific sector.

        Args:
            sector: Sector identifier (total, manufacturing, mining, utilities)

        Returns:
            SectorUtilization with analysis, or None if no data
        """
        if sector not in self.sectors:
            return None

        config = self.sectors[sector]
        series = await self.fetch_series(
            config["series_id"],
            lookback_days=self.lookback_days,
        )

        if series is None or len(series) < 12:
            logger.warning(
                "Insufficient capacity utilization data",
                sector=sector,
                data_points=len(series) if series is not None else 0,
            )
            return None

        # FRED capacity utilization is a percentage (e.g., 78.5)
        current = float(series.iloc[-1]) / 100
        historical = series / 100  # Convert to decimals

        historical_avg = float(historical.mean())
        historical_std = float(historical.std())
        deviation = (current - historical_avg) * 100  # In percentage points

        # Z-score
        z_score = self.calculate_z_score(current, historical_avg, historical_std)

        is_above_threshold = current >= config["threshold"]
        is_critical = current >= config["critical_threshold"]

        if is_critical:
            status = "critical"
        elif is_above_threshold:
            status = "elevated"
        else:
            status = "normal"

        if current >= config["critical_threshold"]:
            score = 80 + (current - config["critical_threshold"]) / 0.05 * 20
        elif current >= config["threshold"]:
            base_score = 60
            range_size = config["critical_threshold"] - config["threshold"]
            if range_size <= 0:
                score = 80.0
            else:
                progress = (current - config["threshold"]) / range_size
                score = base_score + progress * 20
        else:
            score = current / config["threshold"] * 60

        score = min(100.0, max(0.0, score))

        return SectorUtilization(
            sector=sector,
            sector_name=config["name"],
            current_utilization=current,
            threshold=config["threshold"],
            historical_avg=historical_avg,
            deviation_from_avg=deviation,
            z_score=z_score,
            is_above_threshold=is_above_threshold,
            score=score,
            status=status,
        )

    async def get_all_sector_utilizations(self) -> dict[str, SectorUtilization]:
        """Get utilization for all sectors.

        Returns:
            Dictionary of sector to SectorUtilization
        """
        results = {}
        for sector in self.sectors:
            util = await self.get_sector_utilization(sector)
            if util is not None:
                results[sector] = util
        return results

    async def check_sector_thresholds(self) -> list[BottleneckSignalData]:
        """Check all sectors and generate signals for threshold breaches.

        Returns:
            List of BottleneckSignalData for sectors above threshold
        """
        utilizations = await self.get_all_sector_utilizations()
        signals = []

        for sector, util in utilizations.items():
            if util.is_above_threshold:
                severity = min(1.0, util.score / 100)
                confidence = 0.85 if util.status == "critical" else 0.75

                signal = BottleneckSignalData(
                    category=BottleneckCategory.CAPACITY_CEILING,
                    subcategory=f"{sector}_capacity_constraint",
                    severity=severity,
                    confidence=confidence,
                    affected_sectors=[sector.upper()],
                    source_series=[self.sectors[sector]["series_id"]],
                    evidence={
                        "current_utilization": util.current_utilization,
                        "threshold": util.threshold,
                        "historical_avg": util.historical_avg,
                        "deviation_from_avg": util.deviation_from_avg,
                        "z_score": util.z_score,
                    },
                    description=(
                        f"{util.sector_name} capacity utilization at {util.current_utilization:.1%}, "
                        f"above {util.threshold:.0%} threshold "
                        f"({util.deviation_from_avg:+.1f}pp vs. historical avg)"
                    ),
                )
                signals.append(signal)

        return signals

    async def calculate_historical_deviation(self) -> dict[str, dict[str, float]]:
        """Calculate deviation from historical averages for all sectors.

        Returns:
            Dictionary of sector to deviation metrics
        """
        results = {}

        for sector, config in self.sectors.items():
            series = await self.fetch_series(
                config["series_id"],
                lookback_days=self.lookback_days,
            )

            if series is None or len(series) < 12:
                results[sector] = {
                    "current": None,
                    "avg_1y": None,
                    "avg_5y": None,
                    "min_5y": None,
                    "max_5y": None,
                }
                continue

            current = float(series.iloc[-1])
            avg_1y = float(series.tail(12).mean())
            avg_5y = float(series.mean())
            min_5y = float(series.min())
            max_5y = float(series.max())

            results[sector] = {
                "current": current,
                "avg_1y": avg_1y,
                "avg_5y": avg_5y,
                "min_5y": min_5y,
                "max_5y": max_5y,
                "vs_1y_avg": current - avg_1y,
                "vs_5y_avg": current - avg_5y,
                "percentile_5y": float((series < current).sum() / len(series) * 100),
            }

        return results

    async def get_rate_of_change(self) -> dict[str, dict[str, float]]:
        """Calculate rate of change in utilization.

        Returns:
            Dictionary with MoM and YoY changes
        """
        results = {}

        for sector, config in self.sectors.items():
            series = await self.fetch_series(
                config["series_id"],
                lookback_days=self.lookback_days,
            )

            if series is None or len(series) < 13:
                results[sector] = {"mom_change": None, "yoy_change": None}
                continue

            current = float(series.iloc[-1])
            month_ago = float(series.iloc[-2]) if len(series) >= 2 else current
            year_ago = float(series.iloc[-12]) if len(series) >= 12 else current

            results[sector] = {
                "mom_change": current - month_ago,
                "yoy_change": current - year_ago,
                "mom_pct": (current - month_ago) / month_ago * 100 if month_ago > 0 else 0,
                "yoy_pct": (current - year_ago) / year_ago * 100 if year_ago > 0 else 0,
            }

        return results

    async def calculate_score(self) -> MonitorResult:
        """Calculate composite capacity utilization score.

        Returns:
            MonitorResult with composite score and evidence
        """
        utilizations = await self.get_all_sector_utilizations()

        if not utilizations:
            return MonitorResult(
                score=0.0,
                severity=0.0,
                confidence=0.0,
                description="No capacity utilization data available",
            )

        components = {}
        weights = {}

        for sector, util in utilizations.items():
            components[sector] = util.score
            weights[sector] = self.sectors[sector]["weight"]

        composite_score = self.weighted_composite(components, weights)

        elevated_sectors = [s for s, u in utilizations.items() if u.status == "elevated"]
        critical_sectors = [s for s, u in utilizations.items() if u.status == "critical"]

        if critical_sectors:
            sector_names = [utilizations[s].sector_name for s in critical_sectors]
            description = f"Critical capacity constraints in: {', '.join(sector_names)}"
        elif elevated_sectors:
            sector_names = [utilizations[s].sector_name for s in elevated_sectors]
            description = f"Elevated capacity utilization in: {', '.join(sector_names)}"
        else:
            total_util = utilizations.get("total")
            if total_util:
                description = f"Capacity utilization at {total_util.current_utilization:.1%}, within normal range"
            else:
                description = "Capacity utilization within normal ranges"

        data_completeness = len(utilizations) / len(self.sectors)
        confidence = self.calculate_confidence(
            data_completeness=data_completeness,
            sample_size=12,
            min_samples=6,
        )

        return MonitorResult(
            score=composite_score,
            severity=self.score_to_severity(composite_score),
            confidence=confidence,
            components=components,
            evidence={
                "sector_utilizations": {
                    k: v.to_dict() for k, v in utilizations.items()
                },
                "elevated_sectors": elevated_sectors,
                "critical_sectors": critical_sectors,
            },
            description=description,
        )

    async def generate_signals(self) -> list[BottleneckSignalData]:
        """Generate bottleneck signals for capacity constraints.

        Returns:
            List of BottleneckSignalData
        """
        result = await self.calculate_score()
        signals = []

        if result.score >= self.alert_threshold:
            signal = BottleneckSignalData(
                category=BottleneckCategory.CAPACITY_CEILING,
                subcategory="capacity_utilization_composite",
                severity=result.severity,
                confidence=result.confidence,
                affected_sectors=["MANUFACTURING", "MINING", "UTILITIES"],
                source_series=[cfg["series_id"] for cfg in self.sectors.values()],
                evidence={
                    "composite_score": result.score,
                    "components": result.components,
                    **result.evidence,
                },
                description=result.description,
            )
            signals.append(signal)

        sector_signals = await self.check_sector_thresholds()
        signals.extend(sector_signals)

        return signals

    async def get_summary(self) -> dict[str, Any]:
        """Get comprehensive capacity utilization summary.

        Returns:
            Summary dictionary
        """
        utilizations = await self.get_all_sector_utilizations()
        result = await self.calculate_score()
        historical = await self.calculate_historical_deviation()
        rate_of_change = await self.get_rate_of_change()

        return {
            "composite_score": result.score,
            "alert_level": result.alert_level,
            "sectors": {k: v.to_dict() for k, v in utilizations.items()},
            "historical_deviation": historical,
            "rate_of_change": rate_of_change,
            "elevated_count": len(result.evidence.get("elevated_sectors", [])),
            "critical_count": len(result.evidence.get("critical_sectors", [])),
            "timestamp": datetime.now(UTC).isoformat(),
        }
