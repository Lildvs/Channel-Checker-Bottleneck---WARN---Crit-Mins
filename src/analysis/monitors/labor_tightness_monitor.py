"""Labor Tightness Monitor for detecting labor market constraints.

Uses a stock/flow fusion model combining structural and dynamic indicators:

Stock indicators (structural tightness):
- JTSJOL / UNEMPLOY: Beveridge curve vacancy-unemployment ratio
- UNRATE: Unemployment rate
- FRBKCLMCILA: KC Fed LMCI Level of Activity (composite of 24 BLS variables)

Flow indicators (dynamic tightness):
- JTSQUR: Quits Rate (worker confidence)
- JTSHIR - JTSTSR: Net hiring flow
- FRBKCLMCIM: KC Fed LMCI Momentum
- ADPMNUSNERSA: ADP private employment change
- CES0500000003: Average Hourly Earnings (wage pressure)

IMPORTANT: FRED reports JTSQUR, JTSHIR, JTSTSR, and UNRATE as percentage
numbers (e.g., 2.3 means 2.3%), NOT decimals.
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
class LaborMetrics:
    """Labor market metrics for a point in time."""

    job_openings_thousands: float | None
    unemployed_thousands: float | None
    openings_ratio: float | None  # Openings per unemployed (dimensionless)
    quits_rate: float | None  # FRED % (2.3 = 2.3%)
    wage_growth_yoy: float | None  # YoY decimal ratio (0.04 = 4%)
    unemployment_rate: float | None  # FRED % (4.0 = 4.0%)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_openings_thousands": self.job_openings_thousands,
            "unemployed_thousands": self.unemployed_thousands,
            "openings_ratio": self.openings_ratio,
            "quits_rate": self.quits_rate,
            "wage_growth_yoy": self.wage_growth_yoy,
            "unemployment_rate": self.unemployment_rate,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SectorLaborStatus:
    """Labor status for a specific sector."""

    sector: str
    tightness_score: float  # 0-100
    unemployment_rate: float | None
    job_openings_rate: float | None
    wage_growth: float | None
    status: str  # "tight", "normal", "loose"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sector": self.sector,
            "tightness_score": self.tightness_score,
            "unemployment_rate": self.unemployment_rate,
            "job_openings_rate": self.job_openings_rate,
            "wage_growth": self.wage_growth,
            "status": self.status,
        }


# FRED Series IDs
FRED_SERIES = {
    # Stock indicators
    "job_openings": "JTSJOL",  # Job Openings: Total Nonfarm (thousands)
    "unemployed_level": "UNEMPLOY",  # Unemployment Level (thousands)
    "unemployment_rate": "UNRATE",  # Unemployment Rate (%)
    "lmci_level": "FRBKCLMCILA",  # KC Fed LMCI -- Level of Activity
    # Flow indicators
    "quits_rate": "JTSQUR",  # Quits Rate: Total Nonfarm (%)
    "hires_rate": "JTSHIR",  # Hires Rate: Total Nonfarm (%)
    "separations_rate": "JTSTSR",  # Total Separations Rate (%)
    "lmci_momentum": "FRBKCLMCIM",  # KC Fed LMCI -- Momentum
    "adp_employment": "ADPMNUSNERSA",  # ADP National Employment (persons, SA)
    "payrolls": "PAYEMS",  # Total Nonfarm Payrolls
    "avg_hourly_earnings": "CES0500000003",  # Average Hourly Earnings
}

# Sector-specific series (JOLTS by industry)
SECTOR_SERIES = {
    "manufacturing": {
        "job_openings": "JTS3000JOL",
        "quits_rate": "JTS3000QUR",
        "name": "Manufacturing",
    },
    "healthcare": {
        "job_openings": "JTS6200JOL",
        "quits_rate": "JTS6200QUR",
        "name": "Health Care and Social Assistance",
    },
    "construction": {
        "job_openings": "JTS2300JOL",
        "quits_rate": "JTS2300QUR",
        "name": "Construction",
    },
    "retail": {
        "job_openings": "JTS4400JOL",
        "quits_rate": "JTS4400QUR",
        "name": "Retail Trade",
    },
    "transportation": {
        "job_openings": "JTS4800JOL",
        "quits_rate": "JTS4800QUR",
        "name": "Transportation and Warehousing",
    },
}

# Thresholds for labor market tightness
# IMPORTANT: FRED delivers JTSQUR, UNRATE, JTSHIR, JTSTSR as percentage numbers
# (e.g., 2.3 means 2.3%), NOT decimals (0.023). Thresholds use the same convention.
THRESHOLDS = {
    "openings_ratio_tight": 1.2,  # >1.2 V/U ratio = tight (BLS standard)
    "openings_ratio_very_tight": 1.8,  # Very tight (pre-Great Resignation ~2.0)
    "quits_rate_high": 2.5,  # 2.5% quits rate = elevated (FRED %)
    "quits_rate_very_high": 3.0,  # 3.0% = Great Resignation territory (FRED %)
    "wage_growth_elevated": 0.04,  # 4% YoY = elevated (computed as decimal ratio)
    "wage_growth_high": 0.05,  # 5% YoY = high (computed as decimal ratio)
    "unemployment_low": 4.0,  # 4.0% = low unemployment (FRED %)
    "unemployment_very_low": 3.5,  # 3.5% = very low (FRED %)
}

# Component weights for composite score
# Stock = structural tightness (60%), Flow = dynamic tightness (40%)
COMPONENT_WEIGHTS = {
    # Stock (60%)
    "openings_ratio": 0.25,  # Beveridge V/U ratio
    "unemployment": 0.15,  # Unemployment rate
    "lmci_level": 0.20,  # KC Fed composite level
    # Flow (40%)
    "quits_rate": 0.12,  # Worker confidence
    "net_hiring": 0.10,  # Hires - Separations
    "wage_growth": 0.08,  # Wage pressure
    "lmci_momentum": 0.10,  # KC Fed momentum
}


class LaborTightnessMonitor(BaseMonitor):
    """Monitor for labor market tightness and constraints.

    Calculates a composite labor tightness score (0-100) based on:
    - Job openings to unemployed ratio (JOLTS)
    - Quits rate (worker confidence)
    - Wage growth acceleration
    - Unemployment rate
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        lookback_days: int = 365,
        **kwargs,
    ):
        """Initialize the monitor.

        Args:
            db: Database connection
            lookback_days: Days of history for analysis
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(db=db, **kwargs)
        self.lookback_days = lookback_days
        self.thresholds = THRESHOLDS.copy()
        self.weights = COMPONENT_WEIGHTS.copy()

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.LABOR_TIGHTNESS

    async def calculate_openings_ratio(self) -> tuple[float | None, float | None, float | None]:
        """Calculate job openings to unemployed ratio.

        Returns:
            Tuple of (ratio, job_openings, unemployed) or (None, None, None) if data unavailable
        """
        job_openings_series = await self.fetch_series(
            FRED_SERIES["job_openings"],
            lookback_days=self.lookback_days,
        )
        unemployed_series = await self.fetch_series(
            FRED_SERIES["unemployed_level"],
            lookback_days=self.lookback_days,
        )

        if job_openings_series is None or unemployed_series is None:
            logger.warning("No job openings or unemployed data available, returning None")
            return None, None, None

        job_openings = float(job_openings_series.iloc[-1])
        unemployed = float(unemployed_series.iloc[-1])

        if unemployed <= 0:
            ratio = 0.0
        else:
            ratio = job_openings / unemployed

        return ratio, job_openings, unemployed

    async def calculate_quits_indicator(self) -> float | None:
        """Calculate quits rate indicator.

        Returns:
            Quits rate in FRED percentage units (e.g., 2.3 for 2.3%) or None
        """
        quits_series = await self.fetch_series(
            FRED_SERIES["quits_rate"],
            lookback_days=self.lookback_days,
        )

        if quits_series is None:
            logger.warning("No quits rate data available, returning None")
            return None

        # FRED reports as percentage number (2.3 = 2.3%)
        return float(quits_series.iloc[-1])

    async def calculate_wage_acceleration(self) -> float | None:
        """Calculate wage growth (YoY change in average hourly earnings).

        Returns:
            YoY wage growth as decimal (e.g., 0.04 for 4%) or None if data unavailable
        """
        earnings_series = await self.fetch_series(
            FRED_SERIES["avg_hourly_earnings"],
            lookback_days=self.lookback_days + 365,  # Need extra year for YoY
        )

        if earnings_series is None or len(earnings_series) < 13:
            logger.warning("Insufficient earnings data for YoY calculation, returning None")
            return None

        current = float(earnings_series.iloc[-1])
        year_ago = float(earnings_series.iloc[-12]) if len(earnings_series) >= 12 else current
        
        if year_ago <= 0:
            return 0.0
        
        yoy_change = (current - year_ago) / year_ago
        return yoy_change

    async def get_unemployment_rate(self) -> float | None:
        """Get current unemployment rate.

        Returns:
            Unemployment rate in FRED percentage units (e.g., 4.0 for 4.0%) or None
        """
        unrate_series = await self.fetch_series(
            FRED_SERIES["unemployment_rate"],
            lookback_days=self.lookback_days,
        )

        if unrate_series is None:
            logger.warning("No unemployment rate data available, returning None")
            return None

        # FRED reports as percentage number (4.0 = 4.0%)
        return float(unrate_series.iloc[-1])

    async def get_labor_metrics(self) -> LaborMetrics | None:
        """Get comprehensive labor market metrics.

        Returns:
            LaborMetrics with current values, or None if no data available
        """
        ratio, job_openings, unemployed = await self.calculate_openings_ratio()
        quits_rate = await self.calculate_quits_indicator()
        wage_growth = await self.calculate_wage_acceleration()
        unemployment_rate = await self.get_unemployment_rate()

        if all(v is None for v in [ratio, quits_rate, wage_growth, unemployment_rate]):
            logger.warning("No labor metrics data available")
            return None

        return LaborMetrics(
            job_openings_thousands=job_openings,
            unemployed_thousands=unemployed,
            openings_ratio=ratio,
            quits_rate=quits_rate,
            wage_growth_yoy=wage_growth,
            unemployment_rate=unemployment_rate,
        )

    async def get_sector_constraints(self) -> dict[str, SectorLaborStatus]:
        """Get labor market status by sector.

        Returns:
            Dictionary of sector to SectorLaborStatus
        """
        results = {}

        for sector_id, config in SECTOR_SERIES.items():
            openings_series = await self.fetch_series(
                config["job_openings"],
                lookback_days=self.lookback_days,
            )
            quits_series = await self.fetch_series(
                config["quits_rate"],
                lookback_days=self.lookback_days,
            )

            if openings_series is not None and quits_series is not None:
                # FRED reports both as percentage numbers (2.3 = 2.3%)
                quits_rate = float(quits_series.iloc[-1])
                openings_rate = float(openings_series.iloc[-1])

                # Calculate tightness score (both inputs in FRED % units)
                quits_score = min(100.0, quits_rate / 4.0 * 100)  # 4.0% quits = max
                openings_score = min(100.0, openings_rate / 10.0 * 100)  # 10.0% openings rate = max
                tightness_score = (quits_score + openings_score) / 2

                if tightness_score >= 70:
                    status = "tight"
                elif tightness_score >= 40:
                    status = "normal"
                else:
                    status = "loose"

                results[sector_id] = SectorLaborStatus(
                    sector=config["name"],
                    tightness_score=tightness_score,
                    unemployment_rate=None,
                    job_openings_rate=openings_rate,
                    wage_growth=None,
                    status=status,
                )
            else:
                # No data available for this sector - skip it
                logger.warning("No labor data available for sector", sector=sector_id)
                continue

        return results

    def _score_openings_ratio(self, ratio: float) -> float:
        """Convert V/U ratio to 0-100 score.

        Higher ratio = tighter labor market = higher score.
        BLS considers >1.2 as tight market territory.
        """
        very_tight = self.thresholds["openings_ratio_very_tight"]  # 1.8
        tight = self.thresholds["openings_ratio_tight"]  # 1.2

        if ratio >= very_tight:
            return 100.0
        elif ratio >= tight:
            return 60 + (ratio - tight) / (very_tight - tight) * 40
        elif ratio >= 1.0:
            return 30 + (ratio - 1.0) / (tight - 1.0) * 30
        else:
            return max(0.0, ratio / 1.0 * 30)

    def _score_quits_rate(self, quits_rate: float) -> float:
        """Convert quits rate to 0-100 score.

        FRED reports as percentage number (2.3 = 2.3%).
        Higher quits = tighter market (workers confident to leave).
        """
        very_high = self.thresholds["quits_rate_very_high"]  # 3.0%
        high = self.thresholds["quits_rate_high"]  # 2.5%

        if quits_rate >= very_high:
            return 100.0
        elif quits_rate >= high:
            return 70 + (quits_rate - high) / (very_high - high) * 30
        elif quits_rate >= 2.0:
            return 30 + (quits_rate - 2.0) / (high - 2.0) * 40
        elif quits_rate >= 1.5:
            return (quits_rate - 1.5) / 0.5 * 30
        else:
            return 0.0

    def _score_wage_growth(self, wage_growth: float) -> float:
        """Convert wage growth to 0-100 score.

        wage_growth is a YoY decimal ratio (0.04 = 4%).
        Higher wage growth = tighter market (wage pressure).
        """
        if wage_growth >= self.thresholds["wage_growth_high"]:
            return 100.0
        elif wage_growth >= self.thresholds["wage_growth_elevated"]:
            return 70 + (wage_growth - 0.04) / 0.01 * 30
        elif wage_growth >= 0.02:
            return 30 + (wage_growth - 0.02) / 0.02 * 40
        else:
            return max(0, wage_growth / 0.02 * 30)

    def _score_unemployment(self, unemployment_rate: float) -> float:
        """Convert unemployment rate to 0-100 score.

        FRED reports as percentage number (4.0 = 4.0%).
        Lower unemployment = tighter market = higher score.
        """
        very_low = self.thresholds["unemployment_very_low"]  # 3.5%
        low = self.thresholds["unemployment_low"]  # 4.0%

        if unemployment_rate <= very_low:
            return 100.0
        elif unemployment_rate <= low:
            return 70 + (low - unemployment_rate) / (low - very_low) * 30
        elif unemployment_rate <= 6.0:
            return 30 + (6.0 - unemployment_rate) / (6.0 - low) * 40
        elif unemployment_rate <= 10.0:
            return max(0, (10.0 - unemployment_rate) / 4.0 * 30)
        else:
            return 0.0

    async def calculate_score(self) -> MonitorResult:
        """Calculate composite labor tightness score.

        Returns:
            MonitorResult with composite score and evidence
        """
        metrics = await self.get_labor_metrics()
        sector_constraints = await self.get_sector_constraints()

        # Handle case where no labor metrics data is available
        if metrics is None:
            logger.warning("No labor metrics available for scoring")
            return MonitorResult(
                score=0.0,
                severity=0.0,
                confidence=0.0,
                components={},
                evidence={
                    "metrics": None,
                    "sector_constraints": {
                        k: v.to_dict() for k, v in sector_constraints.items()
                    },
                    "tight_sectors": [],
                    "data_unavailable": True,
                },
                description="No labor market data available",
            )

        components: dict[str, float] = {}
        available_weights: dict[str, float] = {}

        if metrics.openings_ratio is not None:
            components["openings_ratio"] = self._score_openings_ratio(metrics.openings_ratio)
            available_weights["openings_ratio"] = self.weights["openings_ratio"]

        if metrics.quits_rate is not None:
            components["quits_rate"] = self._score_quits_rate(metrics.quits_rate)
            available_weights["quits_rate"] = self.weights["quits_rate"]

        if metrics.wage_growth_yoy is not None:
            components["wage_growth"] = self._score_wage_growth(metrics.wage_growth_yoy)
            available_weights["wage_growth"] = self.weights["wage_growth"]

        if metrics.unemployment_rate is not None:
            components["unemployment"] = self._score_unemployment(metrics.unemployment_rate)
            available_weights["unemployment"] = self.weights["unemployment"]

        if not components:
            composite_score = 0.0
        else:
            composite_score = self.weighted_composite(components, available_weights)

        tight_sectors = [
            s.sector for s in sector_constraints.values() if s.status == "tight"
        ]

        if not components:
            description = "Insufficient data to assess labor market conditions"
        elif composite_score >= 80 and metrics.openings_ratio is not None:
            description = (
                f"Very tight labor market: V/U ratio {metrics.openings_ratio:.2f}"
            )
            if metrics.quits_rate is not None:
                description += f", quits {metrics.quits_rate:.1f}%"
            if metrics.wage_growth_yoy is not None:
                description += f", wage growth {metrics.wage_growth_yoy:.1%}"
        elif composite_score >= 60:
            description = "Tight labor market conditions"
            if metrics.openings_ratio is not None:
                description += f": V/U ratio {metrics.openings_ratio:.2f}"
        elif composite_score >= 40:
            description = "Labor market conditions are balanced"
        else:
            description = "Labor market showing signs of slack"

        if tight_sectors:
            description += f". Tight sectors: {', '.join(tight_sectors[:3])}"

        data_completeness = len(components) / len(self.weights)
        confidence = self.calculate_confidence(
            data_completeness=data_completeness,
            sample_size=12,  # Monthly data
            min_samples=6,
        )

        return MonitorResult(
            score=composite_score,
            severity=self.score_to_severity(composite_score),
            confidence=confidence,
            components=components,
            evidence={
                "metrics": metrics.to_dict(),
                "sector_constraints": {
                    k: v.to_dict() for k, v in sector_constraints.items()
                },
                "tight_sectors": tight_sectors,
            },
            description=description,
        )

    async def generate_signals(self) -> list[BottleneckSignalData]:
        """Generate bottleneck signals for labor tightness.

        Returns:
            List of BottleneckSignalData
        """
        result = await self.calculate_score()
        signals = []

        if result.score >= self.alert_threshold:
            metrics = result.evidence.get("metrics", {})

            signal = BottleneckSignalData(
                category=BottleneckCategory.LABOR_TIGHTNESS,
                subcategory="labor_market_tightness",
                severity=result.severity,
                confidence=result.confidence,
                affected_sectors=["MANUFACTURING", "HEALTHCARE", "CONSTRUCTION", "RETAIL"],
                source_series=list(FRED_SERIES.values()),
                evidence={
                    "composite_score": result.score,
                    "components": result.components,
                    "openings_ratio": metrics.get("openings_ratio"),
                    "quits_rate": metrics.get("quits_rate"),
                    "wage_growth_yoy": metrics.get("wage_growth_yoy"),
                },
                description=result.description,
            )
            signals.append(signal)

        return signals

    async def get_sector_summary(self) -> dict[str, Any]:
        """Get summary of labor conditions by sector.

        Returns:
            Summary dictionary
        """
        sector_constraints = await self.get_sector_constraints()
        result = await self.calculate_score()

        return {
            "composite_score": result.score,
            "alert_level": result.alert_level,
            "sectors": {
                k: v.to_dict() for k, v in sector_constraints.items()
            },
            "tight_sector_count": sum(
                1 for s in sector_constraints.values() if s.status == "tight"
            ),
            "timestamp": datetime.now(UTC).isoformat(),
        }
