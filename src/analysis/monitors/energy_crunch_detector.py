"""Energy Crunch Detector for monitoring energy supply stress.

Monitors:
- SPR (Strategic Petroleum Reserve) levels
- Commercial crude + product inventories vs. seasonal norms
- Refinery utilization rates
- Price + storage composite signals

Generates alerts for energy supply vulnerabilities.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import structlog

from src.analysis.monitors.base_monitor import BaseMonitor, MonitorResult
from src.analysis.signals import BottleneckCategory, BottleneckSignalData
from src.storage.timescale import TimescaleDB

logger = structlog.get_logger()


@dataclass
class SPRStatus:
    """Status of Strategic Petroleum Reserve."""

    current_level_mb: float  # Million barrels
    historical_avg_mb: float
    pct_of_historical: float
    days_of_supply: float | None  # None when consumption data unavailable
    is_critical: bool
    score: float  # 0-100 (higher = more stress)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "current_level_mb": self.current_level_mb,
            "historical_avg_mb": self.historical_avg_mb,
            "pct_of_historical": self.pct_of_historical,
            "days_of_supply": self.days_of_supply,
            "is_critical": self.is_critical,
            "score": self.score,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class RefineryStatus:
    """Status of refinery utilization."""

    utilization_rate: float  # 0-1
    capacity_operable_kbd: float | None  # None when capacity data unavailable
    is_high_utilization: bool
    score: float  # 0-100
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "utilization_rate": self.utilization_rate,
            "capacity_operable_kbd": self.capacity_operable_kbd,
            "is_high_utilization": self.is_high_utilization,
            "score": self.score,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SeasonalDeviation:
    """Deviation of inventory from seasonal norms."""

    product: str
    current_level: float
    seasonal_avg: float
    deviation_pct: float
    is_below_normal: bool
    percentile: float  # Where current falls in 5-year range
    score: float  # 0-100
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "product": self.product,
            "current_level": self.current_level,
            "seasonal_avg": self.seasonal_avg,
            "deviation_pct": self.deviation_pct,
            "is_below_normal": self.is_below_normal,
            "percentile": self.percentile,
            "score": self.score,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class EnergyCrunchResult:
    """Result of energy crunch analysis."""

    score: float  # 0-100 composite score
    alert_level: str
    spr_status: SPRStatus | None
    refinery_status: RefineryStatus | None
    inventory_deviations: list[SeasonalDeviation]
    components: dict[str, float]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "score": self.score,
            "alert_level": self.alert_level,
            "spr_status": self.spr_status.to_dict() if self.spr_status else None,
            "refinery_status": self.refinery_status.to_dict() if self.refinery_status else None,
            "inventory_deviations": [d.to_dict() for d in self.inventory_deviations],
            "components": self.components,
            "timestamp": self.timestamp.isoformat(),
        }


# Threshold configurations
THRESHOLDS = {
    "spr_critical_mb": 350.0,  # Million barrels - critical threshold
    "spr_warning_mb": 450.0,  # Warning threshold
    "spr_historical_avg_mb": 600.0,  # Historical average
    "refinery_utilization_alert": 0.90,  # 90% utilization is high
    "refinery_utilization_critical": 0.95,  # 95% is critical
    "stocks_below_seasonal_pct": 0.10,  # 10% below seasonal is concerning
    "stocks_critical_pct": 0.20,  # 20% below is critical
}

# Component weights
COMPONENT_WEIGHTS = {
    "spr_levels": 0.25,
    "commercial_stocks": 0.35,
    "refinery_utilization": 0.20,
    "price_storage_composite": 0.20,
}

# Petroleum products to monitor
PETROLEUM_PRODUCTS = {
    "crude_oil": {
        "series_id": "WCESTUS1",  # Weekly crude stocks
        "name": "Crude Oil",
        "unit": "thousand_barrels",
    },
    "motor_gasoline": {
        "series_id": "WGTSTUS1",  # Weekly gasoline stocks
        "name": "Motor Gasoline",
        "unit": "thousand_barrels",
    },
    "distillate": {
        "series_id": "WDISTUS1",  # Weekly distillate stocks
        "name": "Distillate Fuel Oil",
        "unit": "thousand_barrels",
    },
    "total_petroleum": {
        "series_id": "WTTSTUS1",  # Weekly total petroleum stocks
        "name": "Total Petroleum",
        "unit": "thousand_barrels",
    },
}


class EnergyCrunchDetector(BaseMonitor):
    """Detector for energy supply crunch conditions.

    Monitors petroleum supply infrastructure including:
    - SPR levels vs. historical norms
    - Commercial inventory vs. 5-year seasonal average
    - Refinery utilization rates
    - Price-storage composite signals
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        lookback_days: int = 365 * 5,  # 5 years for seasonal comparison
        **kwargs,
    ):
        """Initialize the detector.

        Args:
            db: Database connection
            lookback_days: Days of history for seasonal analysis
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(db=db, **kwargs)
        self.lookback_days = lookback_days
        self.thresholds = THRESHOLDS.copy()
        self.weights = COMPONENT_WEIGHTS.copy()

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.ENERGY_CRUNCH

    async def calculate_crunch_score(self) -> EnergyCrunchResult:
        """Calculate the energy crunch composite score.

        Returns:
            EnergyCrunchResult with all components
        """
        components: dict[str, float] = {}
        available_weights: dict[str, float] = {}

        # SPR analysis
        spr_status = await self.check_spr_levels()
        if spr_status is not None:
            components["spr_levels"] = spr_status.score
            available_weights["spr_levels"] = self.weights["spr_levels"]

        # Refinery utilization
        refinery_status = await self.check_refinery_utilization()
        if refinery_status is not None:
            components["refinery_utilization"] = refinery_status.score
            available_weights["refinery_utilization"] = self.weights["refinery_utilization"]

        # Seasonal inventory deviations
        inventory_deviations = []
        inventory_scores = []

        for product_id, config in PETROLEUM_PRODUCTS.items():
            deviation = await self.calculate_seasonal_deviation_for_product(
                product_id, config
            )
            if deviation:
                inventory_deviations.append(deviation)
                inventory_scores.append(deviation.score)

        if inventory_scores:
            components["commercial_stocks"] = sum(inventory_scores) / len(inventory_scores)
            available_weights["commercial_stocks"] = self.weights["commercial_stocks"]

        # Price-storage composite
        price_storage_score = await self._calculate_price_storage_composite()
        if price_storage_score is not None:
            components["price_storage_composite"] = price_storage_score
            available_weights["price_storage_composite"] = self.weights["price_storage_composite"]

        if not components:
            logger.warning("No energy data available for crunch score calculation")
            composite_score = 0.0
        else:
            composite_score = self.weighted_composite(components, available_weights)

        if composite_score >= self.critical_threshold:
            alert_level = "critical"
        elif composite_score >= self.alert_threshold:
            alert_level = "elevated"
        else:
            alert_level = "normal"

        return EnergyCrunchResult(
            score=composite_score,
            alert_level=alert_level,
            spr_status=spr_status,
            refinery_status=refinery_status,
            inventory_deviations=inventory_deviations,
            components=components,
        )

    async def check_spr_levels(self) -> SPRStatus | None:
        """Check Strategic Petroleum Reserve levels.

        Returns:
            SPRStatus with current levels and analysis
        """
        # Would query EIA SPR data
        # Placeholder with realistic values
        try:
            # SPR series: WCSSTUS1 (Weekly crude stocks in SPR)
            spr_series = await self.fetch_series("WCSSTUS1", lookback_days=self.lookback_days)
            if spr_series is None or len(spr_series) < 1:
                logger.warning("No SPR level data available")
                return None

            current_level = float(spr_series.iloc[-1]) / 1000  # Convert thousand barrels to million barrels
            historical_avg = self.thresholds["spr_historical_avg_mb"]

            pct_of_historical = (current_level / historical_avg) * 100

            daily_consumption: float | None = None
            try:
                consumption_series = await self.fetch_series("MTTUPUS2", lookback_days=365)
                if consumption_series is not None and len(consumption_series) > 0:
                    # MTTUPUS2 is monthly total petroleum products supplied (thousand barrels/day)
                    daily_consumption = float(consumption_series.iloc[-1]) / 1000  # Convert to million barrels/day
            except Exception as e:
                logger.error(
                    "Failed to fetch petroleum consumption series MTTUPUS2",
                    error=str(e),
                )

            if daily_consumption is None or daily_consumption <= 0:
                logger.error(
                    "CRITICAL: Cannot calculate SPR days-of-supply -- "
                    "EIA series MTTUPUS2 (petroleum consumption) unavailable. "
                    "days_of_supply will be reported as None.",
                )
                days_of_supply = None
            else:
                days_of_supply = current_level / daily_consumption

            is_critical = current_level < self.thresholds["spr_critical_mb"]

            if current_level < self.thresholds["spr_critical_mb"]:
                score = 80 + (
                    (self.thresholds["spr_critical_mb"] - current_level)
                    / self.thresholds["spr_critical_mb"] * 20
                )
            elif current_level < self.thresholds["spr_warning_mb"]:
                score = 50 + (
                    (self.thresholds["spr_warning_mb"] - current_level)
                    / (self.thresholds["spr_warning_mb"] - self.thresholds["spr_critical_mb"])
                    * 30
                )
            else:
                score = (
                    (historical_avg - current_level) / historical_avg * 50
                )
                score = max(0, score)

            score = min(100.0, max(0.0, score))

            return SPRStatus(
                current_level_mb=current_level,
                historical_avg_mb=historical_avg,
                pct_of_historical=pct_of_historical,
                days_of_supply=days_of_supply,
                is_critical=is_critical,
                score=score,
            )

        except Exception as e:
            self.logger.warning("Failed to check SPR levels", error=str(e))
            return None

    async def check_refinery_utilization(self) -> RefineryStatus | None:
        """Check refinery utilization rates.

        Returns:
            RefineryStatus with current utilization
        """
        try:
            # Series: WOCLEUS2 (Weekly refinery utilization)
            refinery_series = await self.fetch_series("WOCLEUS2", lookback_days=self.lookback_days)
            if refinery_series is None or len(refinery_series) < 1:
                logger.warning("No refinery utilization data available")
                return None

            utilization_rate = float(refinery_series.iloc[-1]) / 100  # Convert percentage to decimal

            capacity_kbd: float | None = None
            try:
                capacity_series = await self.fetch_series("MCRFPUS2", lookback_days=365)
                if capacity_series is not None and len(capacity_series) > 0:
                    # MCRFPUS2 is operable refinery capacity (thousand barrels per calendar day)
                    capacity_kbd = float(capacity_series.iloc[-1])
            except Exception as e:
                logger.error(
                    "Failed to fetch refinery capacity series MCRFPUS2",
                    error=str(e),
                )

            if capacity_kbd is None or capacity_kbd <= 0:
                logger.error(
                    "CRITICAL: EIA series MCRFPUS2 (operable refinery capacity) unavailable. "
                    "capacity_operable_kbd will be reported as None.",
                )

            is_high = utilization_rate >= self.thresholds["refinery_utilization_alert"]

            if utilization_rate >= self.thresholds["refinery_utilization_critical"]:
                score = 90 + (utilization_rate - 0.95) * 200
            elif utilization_rate >= self.thresholds["refinery_utilization_alert"]:
                score = 60 + (
                    (utilization_rate - 0.90)
                    / (0.95 - 0.90) * 30
                )
            else:
                score = utilization_rate / 0.90 * 60

            score = min(100.0, max(0.0, score))

            return RefineryStatus(
                utilization_rate=utilization_rate,
                capacity_operable_kbd=capacity_kbd,
                is_high_utilization=is_high,
                score=score,
            )

        except Exception as e:
            self.logger.warning("Failed to check refinery utilization", error=str(e))
            return None

    async def calculate_seasonal_deviation_for_product(
        self,
        product_id: str,
        config: dict[str, Any],
    ) -> SeasonalDeviation | None:
        """Calculate seasonal deviation for a petroleum product.

        Args:
            product_id: Product identifier
            config: Product configuration

        Returns:
            SeasonalDeviation with analysis
        """
        try:
            series = await self.fetch_series(
                config["series_id"],
                lookback_days=self.lookback_days,
            )

            if series is None or len(series) < 52 * 2:
                return None

            current_level = float(series.iloc[-1])

            seasonal_avg, deviation, deviation_pct = self.calculate_seasonal_deviation(
                series=series,
                current_value=current_level,
                period=52,  # Weekly data
                years_back=5,
            )

            five_year_data = series.tail(52 * 5)
            percentile = self.normalize_percentile_to_100(current_level, five_year_data)

            is_below = deviation_pct < -self.thresholds["stocks_below_seasonal_pct"] * 100

            if deviation_pct < -self.thresholds["stocks_critical_pct"] * 100:
                score = 80 + abs(deviation_pct) / 30 * 20
            elif is_below:
                score = 50 + abs(deviation_pct) / 10 * 30
            else:
                # Above or near seasonal average
                score = max(0, 50 - deviation_pct)

            score = min(100.0, max(0.0, score))

            return SeasonalDeviation(
                product=config["name"],
                current_level=current_level,
                seasonal_avg=seasonal_avg,
                deviation_pct=deviation_pct,
                is_below_normal=is_below,
                percentile=percentile,
                score=score,
            )

        except Exception as e:
            self.logger.warning(
                "Failed to calculate seasonal deviation",
                product=product_id,
                error=str(e),
            )
            return None

    async def _calculate_price_storage_composite(self) -> float | None:
        """Calculate price + storage composite signal.

        Higher prices + lower storage = higher stress.

        Returns:
            Composite score 0-100 or None if data unavailable
        """
        try:
            wti_series = await self.fetch_series("DCOILWTICO", lookback_days=365)
            if wti_series is None or len(wti_series) < 30:
                logger.warning("Insufficient WTI price data for composite calculation")
                return None

            current_price = float(wti_series.iloc[-1])
            price_percentile = float((wti_series < current_price).sum() / len(wti_series) * 100)

            return price_percentile

        except Exception as e:
            self.logger.warning("Failed to calculate price-storage composite", error=str(e))
            return None

    async def calculate_days_of_supply(self) -> dict[str, float | None]:
        """Calculate days of supply for key products.

        Returns:
            Dictionary of product to days of supply (None if unavailable)
        """
        results: dict[str, float | None] = {}

        for product_id, config in PETROLEUM_PRODUCTS.items():
            series = await self.fetch_series(config["series_id"], lookback_days=self.lookback_days)
            if series is None or len(series) < 1:
                logger.warning("No inventory data available for days of supply", product=product_id)
                results[product_id] = None
                continue

            current_inventory = float(series.iloc[-1])
            # Use 5-day average consumption estimate (would need actual consumption data)
            # For now, return None as we don't have real consumption data
            logger.warning("Days of supply calculation requires consumption data", product=product_id)
            results[product_id] = None

        return results

    async def calculate_score(self) -> MonitorResult:
        """Calculate the monitor's composite score.

        Returns:
            MonitorResult with score and evidence
        """
        crunch_result = await self.calculate_crunch_score()

        issues = []
        if crunch_result.spr_status and crunch_result.spr_status.is_critical:
            issues.append(
                f"SPR at critical level ({crunch_result.spr_status.current_level_mb:.0f} MB)"
            )
        if crunch_result.refinery_status and crunch_result.refinery_status.is_high_utilization:
            issues.append(
                f"High refinery utilization ({crunch_result.refinery_status.utilization_rate:.0%})"
            )

        below_seasonal = [
            d for d in crunch_result.inventory_deviations if d.is_below_normal
        ]
        if below_seasonal:
            products = ", ".join(d.product for d in below_seasonal[:3])
            issues.append(f"Below-seasonal stocks: {products}")

        if issues:
            description = f"Energy stress indicators: {'; '.join(issues)}"
        else:
            description = "Energy supply indicators within normal ranges"

        data_available = sum([
            1 if crunch_result.spr_status else 0,
            1 if crunch_result.refinery_status else 0,
            len(crunch_result.inventory_deviations) / len(PETROLEUM_PRODUCTS),
        ]) / 3

        confidence = self.calculate_confidence(
            data_completeness=data_available,
            sample_size=52,  # Weekly data for a year
            min_samples=26,
        )

        return MonitorResult(
            score=crunch_result.score,
            severity=self.score_to_severity(crunch_result.score),
            confidence=confidence,
            components=crunch_result.components,
            evidence={
                "alert_level": crunch_result.alert_level,
                "spr_status": crunch_result.spr_status.to_dict() if crunch_result.spr_status else None,
                "refinery_status": crunch_result.refinery_status.to_dict() if crunch_result.refinery_status else None,
                "inventory_deviations": [
                    d.to_dict() for d in crunch_result.inventory_deviations
                ],
                "issues": issues,
            },
            description=description,
        )

    async def generate_signals(self) -> list[BottleneckSignalData]:
        """Generate bottleneck signals for energy crunch conditions.

        Returns:
            List of BottleneckSignalData
        """
        result = await self.calculate_score()
        signals = []

        if result.score >= self.alert_threshold:
            signal = BottleneckSignalData(
                category=BottleneckCategory.ENERGY_CRUNCH,
                subcategory="energy_crunch_composite",
                severity=result.severity,
                confidence=result.confidence,
                affected_sectors=["ENERGY", "TRANSPORTATION", "MANUFACTURING", "UTILITIES"],
                affected_commodities=["crude_oil", "gasoline", "distillate"],
                evidence={
                    "score": result.score,
                    "components": result.components,
                    **result.evidence,
                },
                description=result.description,
            )
            signals.append(signal)

        return signals
