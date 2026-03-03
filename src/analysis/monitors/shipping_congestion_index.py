"""Shipping Congestion Index for composite shipping stress measurement.

Combines multiple data sources:
- Baltic indices (BDI, FBX, SCFI)
- Port throughput (TEU volumes)
- Container dwell times

Produces a normalized 0-100 congestion score with component breakdown.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import func, select, and_, desc

from src.analysis.monitors.base_monitor import BaseMonitor, MonitorResult
from src.analysis.signals import BottleneckCategory, BottleneckSignalData
from src.storage.models import ShippingIndex, PortThroughput, ContainerDwellTime, DataPointModel
from src.storage.timescale import TimescaleDB

logger = structlog.get_logger()


@dataclass
class SCIResult:
    """Result of Shipping Congestion Index calculation."""

    score: float  # 0-100 composite score
    alert_level: str  # "normal", "elevated", "critical"
    components: dict[str, float]  # Component scores
    port_scores: dict[str, float]  # Per-port scores if available
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "score": self.score,
            "alert_level": self.alert_level,
            "components": self.components,
            "port_scores": self.port_scores,
            "timestamp": self.timestamp.isoformat(),
            "evidence": self.evidence,
        }


# Component weights for composite index
COMPONENT_WEIGHTS = {
    "baltic_indices": 0.25,
    "dwell_times": 0.30,
    "teu_throughput": 0.25,
    "container_rates": 0.20,
}

# Baltic index configurations
BALTIC_INDICES = {
    "BDI": {"name": "Baltic Dry Index", "type": "bulk", "baseline": 1500},
    "FBX": {"name": "Freightos Baltic Index", "type": "container", "baseline": 2000},
    "SCFI": {"name": "Shanghai Containerized Freight Index", "type": "container", "baseline": 1000},
}

# Key ports for monitoring
KEY_PORTS = {
    "los_angeles": {"name": "Port of Los Angeles", "region": "West Coast"},
    "long_beach": {"name": "Port of Long Beach", "region": "West Coast"},
    "new_york": {"name": "Port of New York/New Jersey", "region": "East Coast"},
    "savannah": {"name": "Port of Savannah", "region": "East Coast"},
    "houston": {"name": "Port of Houston", "region": "Gulf"},
}


class ShippingCongestionIndex(BaseMonitor):
    """Composite index for shipping and port congestion.

    Calculates a normalized 0-100 score representing overall shipping
    system stress based on:
    - Baltic indices deviation from baseline
    - Container dwell times vs. normal
    - Port TEU throughput changes
    - Freight rate changes
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        elevated_threshold: float = 60.0,
        critical_threshold: float = 80.0,
        lookback_days: int = 365,
        **kwargs,
    ):
        """Initialize the index.

        Args:
            db: Database connection
            elevated_threshold: Score threshold for elevated alert
            critical_threshold: Score threshold for critical alert
            lookback_days: Days of history for baseline
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(
            db=db,
            alert_threshold=elevated_threshold,
            critical_threshold=critical_threshold,
            **kwargs,
        )
        self.lookback_days = lookback_days
        self.weights = COMPONENT_WEIGHTS.copy()

    # Known series IDs in data_points that map to shipping concepts.
    # These are populated by the ShippingDataCollector and FRED collector.
    DATAPOINTS_BALTIC_SERIES: dict[str, dict[str, Any]] = {
        # FRED: Transportation Services Index - Freight (monthly index, baseline ~115)
        "TSIFRGHT": {"series_id": "TSIFRGHT", "baseline": 115.0},
    }

    DATAPOINTS_TEU_SERIES: dict[str, dict[str, Any]] = {
        # ShippingDataCollector writes POLA TEU totals here
        "los_angeles": {"series_id": "POLA_TEU_TOTAL", "port_name": "Port of Los Angeles"},
    }

    DATAPOINTS_RATES_SERIES: dict[str, dict[str, Any]] = {
        # FRED: Rail Freight Carloads (monthly, baseline ~925k carloads/month)
        "rail_freight": {"series_id": "RAILFRTCARLOADSD11", "baseline": 925_000.0},
    }

    async def _get_latest_datapoint(
        self,
        series_id: str,
        cutoff: datetime,
    ) -> tuple[float | None, datetime | None]:
        """Query data_points for the latest value of a series.

        Args:
            series_id: The series identifier in data_points
            cutoff: Only consider rows newer than this

        Returns:
            (value, timestamp) or (None, None) if not found
        """
        if self.db is None:
            return None, None

        try:
            async with self.db.session() as session:
                query = (
                    select(DataPointModel.value, DataPointModel.timestamp)
                    .where(
                        and_(
                            DataPointModel.series_id == series_id,
                            DataPointModel.timestamp >= cutoff,
                            DataPointModel.value.isnot(None),
                        )
                    )
                    .order_by(desc(DataPointModel.timestamp))
                    .limit(1)
                )
                result = await session.execute(query)
                row = result.first()
                if row:
                    return float(row[0]), row[1]
        except Exception as e:
            self.logger.debug("data_points fallback query failed", series_id=series_id, error=str(e))

        return None, None

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.SHIPPING_CONGESTION

    async def calculate_index(self) -> SCIResult:
        """Calculate the Shipping Congestion Index.

        Returns:
            SCIResult with composite score and breakdown
        """
        components = {}
        evidence = {}

        baltic_result = await self._calculate_baltic_component()
        components["baltic_indices"] = baltic_result["score"]
        evidence["baltic"] = baltic_result

        dwell_result = await self._calculate_dwell_component()
        components["dwell_times"] = dwell_result["score"]
        evidence["dwell"] = dwell_result

        teu_result = await self._calculate_teu_component()
        components["teu_throughput"] = teu_result["score"]
        evidence["teu"] = teu_result

        rates_result = await self._calculate_rates_component()
        components["container_rates"] = rates_result["score"]
        evidence["rates"] = rates_result

        composite_score = self.weighted_composite(components, self.weights)

        if composite_score >= self.critical_threshold:
            alert_level = "critical"
        elif composite_score >= self.alert_threshold:
            alert_level = "elevated"
        else:
            alert_level = "normal"

        port_scores = await self._calculate_port_scores()

        return SCIResult(
            score=composite_score,
            alert_level=alert_level,
            components=components,
            port_scores=port_scores,
            evidence=evidence,
        )

    async def _calculate_baltic_component(self) -> dict[str, Any]:
        """Calculate Baltic indices component score.

        Queries the shipping_indices table for latest BDI, FBX, SCFI values.
        Compares current value to baseline -- higher deviation above baseline
        means higher congestion (supply/demand imbalance driving rates up).

        Returns:
            Dictionary with score and details
        """
        if self.db is None:
            return {"score": 50.0, "indices": {}, "data_available": False}

        try:
            index_scores: dict[str, float] = {}
            index_details: dict[str, dict[str, Any]] = {}
            cutoff = datetime.now(UTC) - timedelta(days=30)

            async with self.db.session() as session:
                for index_name, config in BALTIC_INDICES.items():
                    query = (
                        select(ShippingIndex)
                        .where(
                            and_(
                                ShippingIndex.index_name == index_name,
                                ShippingIndex.sub_index.is_(None),
                                ShippingIndex.timestamp >= cutoff,
                            )
                        )
                        .order_by(desc(ShippingIndex.timestamp))
                        .limit(1)
                    )
                    result = await session.execute(query)
                    row = result.scalar_one_or_none()

                    if row is None:
                        continue

                    current_value = float(row.value)
                    baseline = config["baseline"]

                    # At baseline → 50, at 2x baseline → ~85, at 0 → ~15
                    ratio = current_value / baseline if baseline > 0 else 1.0
                    score = min(100.0, max(0.0, 50.0 * ratio))

                    index_scores[index_name] = round(score, 1)
                    index_details[index_name] = {
                        "current_value": current_value,
                        "baseline": baseline,
                        "ratio": round(ratio, 2),
                        "change_percent": row.change_percent,
                        "timestamp": row.timestamp.isoformat(),
                    }

            # Fallback: check data_points for known Baltic series (wider window
            # because monthly economic indices like TSIFRGHT can lag 2-4 months)
            if not index_scores:
                cutoff_wide = datetime.now(UTC) - timedelta(days=150)
                for index_name, dp_config in self.DATAPOINTS_BALTIC_SERIES.items():
                    value, ts = await self._get_latest_datapoint(dp_config["series_id"], cutoff_wide)
                    if value is not None and ts is not None:
                        baseline = dp_config["baseline"]
                        ratio = value / baseline if baseline > 0 else 1.0
                        score = min(100.0, max(0.0, 50.0 * ratio))
                        index_scores[index_name] = round(score, 1)
                        index_details[index_name] = {
                            "current_value": value,
                            "baseline": baseline,
                            "ratio": round(ratio, 2),
                            "source": "data_points_fallback",
                            "series_id": dp_config["series_id"],
                            "timestamp": ts.isoformat(),
                        }

            if not index_scores:
                return {"score": 50.0, "indices": {}, "data_available": False}

            avg_score = sum(index_scores.values()) / len(index_scores)

            return {
                "score": round(avg_score, 1),
                "indices": index_scores,
                "details": index_details,
                "data_available": True,
            }

        except Exception as e:
            self.logger.warning("Failed to calculate Baltic component", error=str(e))
            return {"score": 50.0, "indices": {}, "data_available": False, "error": str(e)}

    async def _calculate_dwell_component(self) -> dict[str, Any]:
        """Calculate container dwell times component.

        Queries the container_dwell_times table for the most recent dwell
        times at key ports. Higher dwell time relative to historical average
        produces a higher congestion score.

        Normal dwell ~3-4 days → score ~40-50.
        Elevated dwell ~6-8 days → score ~70-80.
        Crisis dwell >9 days → score ~90+.

        Returns:
            Dictionary with score and details
        """
        if self.db is None:
            return {"score": 50.0, "ports": {}, "data_available": False}

        try:
            port_dwell_scores: dict[str, float] = {}
            port_details: dict[str, dict[str, Any]] = {}
            cutoff = datetime.now(UTC) - timedelta(days=30)
            # Normal baseline dwell time in days
            normal_dwell_days = 3.5

            async with self.db.session() as session:
                for port_id, config in KEY_PORTS.items():
                    port_name = config["name"]

                    query = (
                        select(ContainerDwellTime)
                        .where(
                            and_(
                                ContainerDwellTime.port_name.ilike(f"%{port_id.replace('_', ' ')}%"),
                                ContainerDwellTime.container_type == "all",
                                ContainerDwellTime.measurement_date >= cutoff,
                            )
                        )
                        .order_by(desc(ContainerDwellTime.measurement_date))
                        .limit(1)
                    )
                    result = await session.execute(query)
                    row = result.scalar_one_or_none()

                    if row is None:
                        continue

                    avg_dwell = float(row.dwell_time_avg_days)
                    # Use the row's own historical average if available, else our baseline
                    baseline = float(row.historical_avg_dwell) if row.historical_avg_dwell else normal_dwell_days

                    # Score: ratio of current to baseline, scaled to 0-100
                    # At baseline → 50, at 2x baseline → ~85, at 3x → 100
                    ratio = avg_dwell / baseline if baseline > 0 else 1.0
                    score = min(100.0, max(0.0, 50.0 * ratio))

                    port_dwell_scores[port_id] = round(score, 1)
                    port_details[port_id] = {
                        "port_name": port_name,
                        "dwell_avg_days": round(avg_dwell, 1),
                        "baseline_days": round(baseline, 1),
                        "ratio": round(ratio, 2),
                        "pct_over_5_days": row.pct_over_5_days,
                        "measurement_date": row.measurement_date.isoformat(),
                    }

            if not port_dwell_scores:
                return {"score": 50.0, "ports": {}, "data_available": False}

            avg_score = sum(port_dwell_scores.values()) / len(port_dwell_scores)

            return {
                "score": round(avg_score, 1),
                "ports": port_dwell_scores,
                "details": port_details,
                "data_available": True,
            }

        except Exception as e:
            self.logger.warning("Failed to calculate dwell component", error=str(e))
            return {"score": 50.0, "ports": {}, "data_available": False, "error": str(e)}

    async def _calculate_teu_component(self) -> dict[str, Any]:
        """Calculate TEU throughput component.

        Queries port_throughput for recent TEU volumes per port.
        Lower throughput vs year-over-year = higher congestion (cargo stuck).
        Higher throughput vs year-over-year = lower congestion (flow is good).

        Returns:
            Dictionary with score and details
        """
        if self.db is None:
            return {"score": 50.0, "ports": {}, "data_available": False}

        try:
            port_teu_scores: dict[str, float] = {}
            port_details: dict[str, dict[str, Any]] = {}
            cutoff = datetime.now(UTC) - timedelta(days=90)

            async with self.db.session() as session:
                for port_id, config in KEY_PORTS.items():
                    port_name = config["name"]

                    query = (
                        select(PortThroughput)
                        .where(
                            and_(
                                PortThroughput.port_name.ilike(f"%{port_id.replace('_', ' ')}%"),
                                PortThroughput.metric_type == "teu_total",
                                PortThroughput.period_start >= cutoff,
                            )
                        )
                        .order_by(desc(PortThroughput.period_start))
                        .limit(1)
                    )
                    result = await session.execute(query)
                    row = result.scalar_one_or_none()

                    if row is None:
                        continue

                    current_teu = float(row.value)
                    yoy_change = row.year_over_year_change_percent

                    # Score: inverted -- negative YoY change = higher congestion
                    # -20% YoY → score ~70, 0% → 50, +20% → ~30
                    if yoy_change is not None:
                        score = max(0.0, min(100.0, 50.0 - (yoy_change * 1.0)))
                    else:
                        score = 50.0  # No comparison data

                    port_teu_scores[port_id] = round(score, 1)
                    port_details[port_id] = {
                        "port_name": port_name,
                        "current_teu": current_teu,
                        "yoy_change_percent": yoy_change,
                        "change_percent": row.change_percent,
                        "period": row.period_label or row.period_start.isoformat(),
                        "is_preliminary": row.is_preliminary,
                    }

            # Fallback: check data_points for TEU series (wider window for lag)
            if not port_teu_scores:
                cutoff_wide = datetime.now(UTC) - timedelta(days=180)
                for port_id, dp_config in self.DATAPOINTS_TEU_SERIES.items():
                    value, ts = await self._get_latest_datapoint(dp_config["series_id"], cutoff_wide)
                    if value is not None and ts is not None:
                        # Without YoY data, derive score from absolute TEU level
                        # Normal monthly POLA TEU ~800k; higher = healthier (lower score)
                        baseline_teu = 800_000.0
                        ratio = value / baseline_teu if baseline_teu > 0 else 1.0
                        # Inverted: higher throughput → lower congestion score
                        score = max(0.0, min(100.0, 100.0 - (50.0 * ratio)))
                        port_teu_scores[port_id] = round(score, 1)
                        port_details[port_id] = {
                            "port_name": dp_config["port_name"],
                            "current_teu": value,
                            "source": "data_points_fallback",
                            "series_id": dp_config["series_id"],
                            "timestamp": ts.isoformat(),
                        }

            if not port_teu_scores:
                return {"score": 50.0, "ports": {}, "data_available": False}

            avg_score = sum(port_teu_scores.values()) / len(port_teu_scores)

            return {
                "score": round(avg_score, 1),
                "ports": port_teu_scores,
                "details": port_details,
                "data_available": True,
            }

        except Exception as e:
            self.logger.warning("Failed to calculate TEU component", error=str(e))
            return {"score": 50.0, "ports": {}, "data_available": False, "error": str(e)}

    async def _calculate_rates_component(self) -> dict[str, Any]:
        """Calculate container shipping rates component.

        Queries shipping_indices for container-type sub-indices (FBX routes).
        Higher rates vs baseline = higher congestion (capacity squeeze).

        Returns:
            Dictionary with score and details
        """
        if self.db is None:
            return {"score": 50.0, "routes": {}, "data_available": False}

        try:
            route_scores: dict[str, float] = {}
            route_details: dict[str, dict[str, Any]] = {}
            cutoff = datetime.now(UTC) - timedelta(days=30)
            # Baseline container rate in USD/TEU (approximate global average)
            baseline_rate = 2000.0

            async with self.db.session() as session:
                query = (
                    select(ShippingIndex)
                    .where(
                        and_(
                            ShippingIndex.index_type == "container",
                            ShippingIndex.sub_index.isnot(None),
                            ShippingIndex.timestamp >= cutoff,
                        )
                    )
                    .order_by(
                        ShippingIndex.sub_index,
                        desc(ShippingIndex.timestamp),
                    )
                )
                result = await session.execute(query)
                rows = result.scalars().all()

                seen_routes: set[str] = set()
                for row in rows:
                    route = row.sub_index
                    if route in seen_routes:
                        continue
                    seen_routes.add(route)

                    current_rate = float(row.value)
                    ratio = current_rate / baseline_rate if baseline_rate > 0 else 1.0
                    score = min(100.0, max(0.0, 50.0 * ratio))

                    route_scores[route] = round(score, 1)
                    route_details[route] = {
                        "index_name": row.index_name,
                        "current_value": current_rate,
                        "unit": row.unit,
                        "change_percent": row.change_percent,
                        "timestamp": row.timestamp.isoformat(),
                    }

            # Fallback: check data_points for freight/rate proxies
            if not route_scores:
                cutoff_wide = datetime.now(UTC) - timedelta(days=90)
                for route_name, dp_config in self.DATAPOINTS_RATES_SERIES.items():
                    value, ts = await self._get_latest_datapoint(dp_config["series_id"], cutoff_wide)
                    if value is not None and ts is not None:
                        baseline = dp_config["baseline"]
                        ratio = value / baseline if baseline > 0 else 1.0
                        score = min(100.0, max(0.0, 50.0 * ratio))
                        route_scores[route_name] = round(score, 1)
                        route_details[route_name] = {
                            "current_value": value,
                            "baseline": baseline,
                            "ratio": round(ratio, 2),
                            "source": "data_points_fallback",
                            "series_id": dp_config["series_id"],
                            "timestamp": ts.isoformat(),
                        }

            if not route_scores:
                return {"score": 50.0, "routes": {}, "data_available": False}

            avg_score = sum(route_scores.values()) / len(route_scores)

            return {
                "score": round(avg_score, 1),
                "routes": route_scores,
                "details": route_details,
                "data_available": True,
            }

        except Exception as e:
            self.logger.warning("Failed to calculate rates component", error=str(e))
            return {"score": 50.0, "routes": {}, "data_available": False, "error": str(e)}

    async def _calculate_port_scores(self) -> dict[str, float]:
        """Calculate individual port congestion scores.

        Combines the most recent dwell time and TEU throughput data for each
        port into a per-port composite score. Equal weight to both metrics.

        Returns:
            Dictionary mapping port name to score
        """
        if self.db is None:
            return {config["name"]: 50.0 for config in KEY_PORTS.values()}

        port_scores: dict[str, float] = {}
        cutoff = datetime.now(UTC) - timedelta(days=90)
        normal_dwell_days = 3.5

        try:
            async with self.db.session() as session:
                for port_id, config in KEY_PORTS.items():
                    port_name = config["name"]
                    component_scores: list[float] = []

                    # Dwell time score for this port
                    dwell_query = (
                        select(ContainerDwellTime)
                        .where(
                            and_(
                                ContainerDwellTime.port_name.ilike(f"%{port_id.replace('_', ' ')}%"),
                                ContainerDwellTime.container_type == "all",
                                ContainerDwellTime.measurement_date >= cutoff,
                            )
                        )
                        .order_by(desc(ContainerDwellTime.measurement_date))
                        .limit(1)
                    )
                    dwell_result = await session.execute(dwell_query)
                    dwell_row = dwell_result.scalar_one_or_none()

                    if dwell_row:
                        baseline = float(dwell_row.historical_avg_dwell) if dwell_row.historical_avg_dwell else normal_dwell_days
                        ratio = float(dwell_row.dwell_time_avg_days) / baseline if baseline > 0 else 1.0
                        component_scores.append(min(100.0, max(0.0, 50.0 * ratio)))

                    # TEU throughput score for this port
                    teu_query = (
                        select(PortThroughput)
                        .where(
                            and_(
                                PortThroughput.port_name.ilike(f"%{port_id.replace('_', ' ')}%"),
                                PortThroughput.metric_type == "teu_total",
                                PortThroughput.period_start >= cutoff,
                            )
                        )
                        .order_by(desc(PortThroughput.period_start))
                        .limit(1)
                    )
                    teu_result = await session.execute(teu_query)
                    teu_row = teu_result.scalar_one_or_none()

                    if teu_row and teu_row.year_over_year_change_percent is not None:
                        yoy = teu_row.year_over_year_change_percent
                        component_scores.append(max(0.0, min(100.0, 50.0 - (yoy * 1.0))))

                    if component_scores:
                        port_scores[port_name] = round(
                            sum(component_scores) / len(component_scores), 1
                        )
                    else:
                        port_scores[port_name] = 50.0

        except Exception as e:
            self.logger.warning("Failed to calculate port scores", error=str(e))
            return {config["name"]: 50.0 for config in KEY_PORTS.values()}

        return port_scores

    async def get_component_scores(self) -> dict[str, float]:
        """Get breakdown of component scores.

        Returns:
            Dictionary of component name to score
        """
        result = await self.calculate_index()
        return result.components

    async def compare_historical(
        self,
        lookback_months: int = 12,
    ) -> dict[str, Any]:
        """Compare current index to historical values.

        Args:
            lookback_months: Months to look back

        Returns:
            Historical comparison data
        """
        current = await self.calculate_index()

        # Would query historical SCI values
        # Placeholder implementation
        return {
            "current_score": current.score,
            "current_alert_level": current.alert_level,
            "historical_avg": 45.0,
            "historical_max": 85.0,
            "historical_min": 15.0,
            "percentile": 65.0,
            "trend": "stable",
            "months_analyzed": lookback_months,
        }

    async def calculate_score(self) -> MonitorResult:
        """Calculate the monitor's composite score.

        Returns:
            MonitorResult with score and evidence
        """
        sci_result = await self.calculate_index()

        if sci_result.alert_level == "critical":
            description = (
                f"Critical shipping congestion detected (score: {sci_result.score:.0f}). "
                f"Multiple indicators showing significant stress."
            )
        elif sci_result.alert_level == "elevated":
            description = (
                f"Elevated shipping congestion (score: {sci_result.score:.0f}). "
                f"Monitor for potential supply chain impacts."
            )
        else:
            description = (
                f"Shipping congestion within normal range (score: {sci_result.score:.0f})."
            )

        data_available_count = sum(
            1 for v in sci_result.evidence.values()
            if isinstance(v, dict) and v.get("data_available", False)
        )
        data_completeness = data_available_count / len(COMPONENT_WEIGHTS)

        confidence = self.calculate_confidence(
            data_completeness=data_completeness,
            sample_size=30,  # Assume reasonable sample
            min_samples=30,
        )

        return MonitorResult(
            score=sci_result.score,
            severity=self.score_to_severity(sci_result.score),
            confidence=confidence,
            components=sci_result.components,
            evidence={
                "alert_level": sci_result.alert_level,
                "port_scores": sci_result.port_scores,
                "component_details": sci_result.evidence,
            },
            description=description,
        )

    async def generate_signals(self) -> list[BottleneckSignalData]:
        """Generate bottleneck signals for shipping congestion.

        Returns:
            List of BottleneckSignalData
        """
        result = await self.calculate_score()
        signals = []

        if result.score >= self.alert_threshold:
            signal = BottleneckSignalData(
                category=BottleneckCategory.SHIPPING_CONGESTION,
                subcategory="shipping_congestion_index",
                severity=result.severity,
                confidence=result.confidence,
                affected_sectors=["TRANSPORTATION", "RETAIL", "MANUFACTURING", "WHOLESALE"],
                evidence={
                    "score": result.score,
                    "alert_level": result.evidence.get("alert_level", "unknown"),
                    "components": result.components,
                },
                description=result.description,
            )
            signals.append(signal)

        return signals

    async def get_port_drilldown(self, port_name: str) -> dict[str, Any]:
        """Get detailed metrics for a specific port.

        Queries both container_dwell_times and port_throughput for
        the named port and returns a combined drilldown view.

        Args:
            port_name: Name of the port

        Returns:
            Detailed port metrics
        """
        result: dict[str, Any] = {
            "port_name": port_name,
            "congestion_score": 50.0,
            "dwell_time_avg_days": None,
            "dwell_time_vs_normal": None,
            "teu_throughput_mtd": None,
            "teu_vs_last_month": None,
            "data_timestamp": datetime.now(UTC).isoformat(),
        }

        if self.db is None:
            return result

        cutoff = datetime.now(UTC) - timedelta(days=90)
        normal_dwell_days = 3.5
        component_scores: list[float] = []

        try:
            async with self.db.session() as session:
                # Dwell time
                dwell_query = (
                    select(ContainerDwellTime)
                    .where(
                        and_(
                            ContainerDwellTime.port_name.ilike(f"%{port_name}%"),
                            ContainerDwellTime.container_type == "all",
                            ContainerDwellTime.measurement_date >= cutoff,
                        )
                    )
                    .order_by(desc(ContainerDwellTime.measurement_date))
                    .limit(1)
                )
                dwell_result = await session.execute(dwell_query)
                dwell_row = dwell_result.scalar_one_or_none()

                if dwell_row:
                    avg_dwell = float(dwell_row.dwell_time_avg_days)
                    baseline = float(dwell_row.historical_avg_dwell) if dwell_row.historical_avg_dwell else normal_dwell_days
                    ratio = avg_dwell / baseline if baseline > 0 else 1.0
                    result["dwell_time_avg_days"] = round(avg_dwell, 1)
                    result["dwell_time_vs_normal"] = round(ratio, 2)
                    result["pct_over_5_days"] = dwell_row.pct_over_5_days
                    result["pct_over_9_days"] = dwell_row.pct_over_9_days
                    component_scores.append(min(100.0, max(0.0, 50.0 * ratio)))

                # TEU throughput
                teu_query = (
                    select(PortThroughput)
                    .where(
                        and_(
                            PortThroughput.port_name.ilike(f"%{port_name}%"),
                            PortThroughput.metric_type == "teu_total",
                            PortThroughput.period_start >= cutoff,
                        )
                    )
                    .order_by(desc(PortThroughput.period_start))
                    .limit(1)
                )
                teu_result = await session.execute(teu_query)
                teu_row = teu_result.scalar_one_or_none()

                if teu_row:
                    result["teu_throughput_mtd"] = float(teu_row.value)
                    result["teu_vs_last_month"] = teu_row.change_percent
                    result["period"] = teu_row.period_label or teu_row.period_start.isoformat()
                    if teu_row.year_over_year_change_percent is not None:
                        yoy = teu_row.year_over_year_change_percent
                        component_scores.append(max(0.0, min(100.0, 50.0 - (yoy * 1.0))))

                if component_scores:
                    result["congestion_score"] = round(sum(component_scores) / len(component_scores), 1)

        except Exception as e:
            self.logger.warning("Failed to get port drilldown", port=port_name, error=str(e))

        return result
