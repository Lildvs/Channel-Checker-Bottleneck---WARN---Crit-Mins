"""Critical Mineral Supply Risk Monitor.

Assesses supply chain risk for critical minerals:
- Lithium, Cobalt, Nickel, Graphite, Rare Earths, Manganese

Risk components:
- Import dependency ratio
- Supplier concentration (Herfindahl-Hirschman Index)
- Price volatility
- Geopolitical risk overlay (China, Russia, DRC exposure)
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
from src.storage.models import MineralProduction, MineralTradeFlow, DataPointModel
from src.storage.timescale import TimescaleDB

logger = structlog.get_logger()


@dataclass
class MineralRiskResult:
    """Risk assessment result for a single mineral."""

    mineral: str
    composite_risk_score: float  # 0-100
    import_dependency: float  # 0-1 ratio
    concentration_risk: float  # 0-100 (from HHI)
    price_volatility: float  # 0-100
    geopolitical_risk: float  # 0-100
    top_suppliers: list[dict[str, Any]]  # Country shares
    alert_level: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "mineral": self.mineral,
            "composite_risk_score": self.composite_risk_score,
            "import_dependency": self.import_dependency,
            "concentration_risk": self.concentration_risk,
            "price_volatility": self.price_volatility,
            "geopolitical_risk": self.geopolitical_risk,
            "top_suppliers": self.top_suppliers,
            "alert_level": self.alert_level,
            "timestamp": self.timestamp.isoformat(),
        }


# Critical minerals configuration
CRITICAL_MINERALS = {
    "lithium": {
        "name": "Lithium",
        "usgs_code": "LI",
        "hs_codes": ["282520"],
        "us_production_pct": 0.01,  # ~1% domestic
        "weight": 0.20,
    },
    "cobalt": {
        "name": "Cobalt",
        "usgs_code": "CO",
        "hs_codes": ["810520"],
        "us_production_pct": 0.0,  # No significant domestic production
        "weight": 0.18,
    },
    "nickel": {
        "name": "Nickel",
        "usgs_code": "NI",
        "hs_codes": ["750210"],
        "us_production_pct": 0.0,  # No significant domestic
        "weight": 0.15,
    },
    "graphite": {
        "name": "Graphite (Natural)",
        "usgs_code": "GR",
        "hs_codes": ["250410"],
        "us_production_pct": 0.0,  # 100% imported
        "weight": 0.15,
    },
    "rare_earths": {
        "name": "Rare Earth Elements",
        "usgs_code": "REE",
        "hs_codes": ["280530"],
        "us_production_pct": 0.12,  # ~12% domestic (Mountain Pass)
        "weight": 0.20,
    },
    "manganese": {
        "name": "Manganese",
        "usgs_code": "MN",
        "hs_codes": ["260200"],
        "us_production_pct": 0.0,  # 100% imported
        "weight": 0.12,
    },
}

# Geopolitical risk weights by country
RISK_COUNTRIES = {
    "CHN": {"name": "China", "risk_weight": 0.80, "description": "Strategic competitor"},
    "RUS": {"name": "Russia", "risk_weight": 0.90, "description": "Sanctioned/adversary"},
    "COD": {"name": "DR Congo", "risk_weight": 0.70, "description": "Political instability"},
    "MMR": {"name": "Myanmar", "risk_weight": 0.75, "description": "Military regime"},
    "ZWE": {"name": "Zimbabwe", "risk_weight": 0.65, "description": "Political risk"},
    "VEN": {"name": "Venezuela", "risk_weight": 0.85, "description": "Sanctioned"},
    "IRN": {"name": "Iran", "risk_weight": 0.95, "description": "Heavily sanctioned"},
    "PRK": {"name": "North Korea", "risk_weight": 1.00, "description": "Fully sanctioned"},
}

# Allied/low-risk countries
LOW_RISK_COUNTRIES = {
    "AUS": 0.10,
    "CAN": 0.10,
    "JPN": 0.15,
    "KOR": 0.15,
    "GBR": 0.10,
    "DEU": 0.15,
    "FRA": 0.15,
    "CHL": 0.25,
    "BRA": 0.30,
    "ARG": 0.30,
    "PHL": 0.35,
    "IDN": 0.40,
    "IND": 0.35,
}

# Risk component weights
COMPONENT_WEIGHTS = {
    "import_dependency": 0.25,
    "concentration_risk": 0.30,
    "price_volatility": 0.20,
    "geopolitical_risk": 0.25,
}


class CriticalMineralRisk(BaseMonitor):
    """Monitor for critical mineral supply chain risks.

    Calculates risk scores based on:
    - Import dependency (US reliance on imports)
    - Supplier concentration (Herfindahl-Hirschman Index)
    - Price volatility (rolling std/mean)
    - Geopolitical risk (exposure to adversary nations)
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
            lookback_days: Days for price volatility analysis
            **kwargs: Additional arguments for BaseMonitor
        """
        super().__init__(db=db, **kwargs)
        self.lookback_days = lookback_days
        self.minerals = CRITICAL_MINERALS.copy()
        self.risk_countries = RISK_COUNTRIES.copy()
        self.low_risk_countries = LOW_RISK_COUNTRIES.copy()
        self.weights = COMPONENT_WEIGHTS.copy()

    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        return BottleneckCategory.SUPPLY_DISRUPTION

    async def calculate_import_dependency(self, mineral: str) -> float:
        """Calculate import dependency ratio for a mineral.

        Import dependency = Imports / (Production + Imports - Exports)

        Args:
            mineral: Mineral identifier

        Returns:
            Import dependency ratio (0-1)
        """
        if mineral not in self.minerals:
            return 0.0

        config = self.minerals[mineral]

        # Use configured US production percentage
        us_production_pct = config.get("us_production_pct", 0.0)

        # Import dependency is inverse of domestic production
        import_dependency = 1.0 - us_production_pct

        # Would also query MineralTradeFlow table for actual import/export data
        # to calculate: imports / (production + imports - exports)

        return import_dependency

    async def calculate_concentration_risk(self, mineral: str) -> tuple[float | None, list[dict]]:
        """Calculate supplier concentration using Herfindahl-Hirschman Index.

        HHI = sum of squared market shares
        - HHI < 1500: Unconcentrated
        - 1500 <= HHI < 2500: Moderately concentrated
        - HHI >= 2500: Highly concentrated

        Args:
            mineral: Mineral identifier

        Returns:
            Tuple of (normalized HHI score 0-100 or None, list of top suppliers)
        """
        supplier_shares = await self._get_supplier_shares(mineral)

        if not supplier_shares:
            logger.warning("No supplier share data available for concentration risk", mineral=mineral)
            return None, []

        shares = list(supplier_shares.values())
        hhi = sum(s ** 2 for s in shares) * 10000  # Scale to standard HHI

        # Normalize to 0-100 (HHI max is 10000 for monopoly)
        normalized = min(100.0, hhi / 100)

        top_suppliers = sorted(
            [
                {"country": k, "share": v, "risk": self._get_country_risk(k)}
                for k, v in supplier_shares.items()
            ],
            key=lambda x: x["share"],
            reverse=True,
        )[:5]

        return normalized, top_suppliers

    async def _get_supplier_shares(self, mineral: str) -> dict[str, float]:
        """Get supplier country shares for a mineral from database.

        Queries MineralProduction for the latest year's production by country,
        then calculates each country's share of global output.  Falls back to
        data_points COMTRADE import series if the dedicated table is empty.

        Returns:
            Dictionary of country ISO3 to market share (0-1), or empty dict if unavailable
        """
        if self.db is None:
            logger.warning(
                "No database connection available for supplier shares",
                mineral=mineral,
            )
            return {}

        try:
            shares = await self._get_shares_from_production(mineral)
            if shares:
                return shares

            shares = await self._get_shares_from_comtrade(mineral)
            if shares:
                return shares

            logger.debug("No supplier share data found", mineral=mineral)
            return {}

        except Exception as e:
            logger.error("Failed to fetch supplier shares", mineral=mineral, error=str(e))
            return {}

    async def _get_shares_from_production(self, mineral: str) -> dict[str, float]:
        """Query MineralProduction table for country production shares.

        Returns:
            Dictionary of country_iso3 to share (0-1), or empty dict
        """
        if self.db is None:
            return {}

        try:
            async with self.db.session() as session:
                year_query = (
                    select(func.max(MineralProduction.year))
                    .where(
                        and_(
                            MineralProduction.mineral == mineral,
                            MineralProduction.production.isnot(None),
                            MineralProduction.production > 0,
                        )
                    )
                )
                result = await session.execute(year_query)
                latest_year = result.scalar_one_or_none()

                if latest_year is None:
                    return {}

                prod_query = (
                    select(
                        MineralProduction.country_iso3,
                        MineralProduction.production,
                    )
                    .where(
                        and_(
                            MineralProduction.mineral == mineral,
                            MineralProduction.year == latest_year,
                            MineralProduction.production.isnot(None),
                            MineralProduction.production > 0,
                            MineralProduction.country_iso3.isnot(None),
                        )
                    )
                )
                result = await session.execute(prod_query)
                rows = result.all()

                if not rows:
                    return {}

                total = sum(float(r[1]) for r in rows)
                if total <= 0:
                    return {}

                shares: dict[str, float] = {}
                for iso3, production in rows:
                    share = float(production) / total
                    if share >= 0.01:  # Only include countries with >= 1% share
                        shares[iso3] = round(share, 4)

                # Lump the rest as OTHER
                remainder = 1.0 - sum(shares.values())
                if remainder > 0.01:
                    shares["OTHER"] = round(remainder, 4)

                return shares

        except Exception as e:
            logger.debug("MineralProduction query failed", mineral=mineral, error=str(e))
            return {}

    async def _get_shares_from_comtrade(self, mineral: str) -> dict[str, float]:
        """Fallback: derive supplier shares from COMTRADE import data in data_points.

        Queries data_points for COMTRADE_{MINERAL}_IMPORT series where the
        reporter is the US.  Groups by partner country (from extra_data JSONB)
        and calculates value shares.

        Returns:
            Dictionary of country ISO3 or name to share (0-1), or empty dict
        """
        if self.db is None:
            return {}

        try:
            series_id = f"COMTRADE_{mineral.upper()}_IMPORT"
            cutoff = datetime.now(UTC) - timedelta(days=730)  # 2 years of trade data

            async with self.db.session() as session:
                query = (
                    select(DataPointModel.value, DataPointModel.extra_data)
                    .where(
                        and_(
                            DataPointModel.series_id == series_id,
                            DataPointModel.timestamp >= cutoff,
                            DataPointModel.value.isnot(None),
                        )
                    )
                )
                result = await session.execute(query)
                rows = result.all()

                if not rows:
                    return {}

                country_values: dict[str, float] = {}
                for value, extra in rows:
                    if not extra or not isinstance(extra, dict):
                        continue
                    partner = extra.get("partner", "Unknown")
                    country_values[partner] = country_values.get(partner, 0.0) + float(value)

                total = sum(country_values.values())
                if total <= 0:
                    return {}

                shares: dict[str, float] = {}
                for country, val in country_values.items():
                    share = val / total
                    if share >= 0.01:
                        shares[country] = round(share, 4)

                remainder = 1.0 - sum(shares.values())
                if remainder > 0.01:
                    shares["OTHER"] = round(remainder, 4)

                return shares

        except Exception as e:
            logger.debug("COMTRADE fallback query failed", mineral=mineral, error=str(e))
            return {}

    def _get_country_risk(self, country_iso3: str) -> float:
        """Get geopolitical risk weight for a country.

        Args:
            country_iso3: ISO3 country code

        Returns:
            Risk weight (0-1)
        """
        if country_iso3 in self.risk_countries:
            return self.risk_countries[country_iso3]["risk_weight"]
        if country_iso3 in self.low_risk_countries:
            return self.low_risk_countries[country_iso3]
        return 0.40  # Default moderate risk for unknown

    async def calculate_geopolitical_risk(self, mineral: str) -> float | None:
        """Calculate geopolitical risk score for a mineral.

        Based on exposure to high-risk supplier countries.

        Args:
            mineral: Mineral identifier

        Returns:
            Geopolitical risk score (0-100) or None if data unavailable
        """
        supplier_shares = await self._get_supplier_shares(mineral)

        if not supplier_shares:
            logger.warning("No supplier share data available for geopolitical risk", mineral=mineral)
            return None

        # Weighted average of country risks by market share
        weighted_risk = 0.0
        for country, share in supplier_shares.items():
            if country == "OTHER":
                risk = 0.40
            else:
                risk = self._get_country_risk(country)
            weighted_risk += share * risk

        # Normalize to 0-100
        return weighted_risk * 100

    async def calculate_price_volatility(self, mineral: str) -> float | None:
        """Calculate price volatility score for a mineral.

        Uses the coefficient of variation (std / mean) of trade values over
        the lookback period as a proxy for price volatility.  Queries the
        data_points table for COMTRADE import value series, or USGS production
        series as a secondary proxy.

        Score mapping: CV of 0 → 0, CV of 0.5 → ~50, CV of 1.0 → 100.

        Args:
            mineral: Mineral identifier

        Returns:
            Price volatility score (0-100) or None if data unavailable
        """
        if self.db is None:
            logger.warning(
                "No database connection available for price volatility",
                mineral=mineral,
            )
            return None

        try:
            # Try COMTRADE import values first (best price proxy)
            series_candidates = [
                f"COMTRADE_{mineral.upper()}_IMPORT",
                f"USGS_{mineral.upper()}_PRODUCTION",
                f"IEA_{mineral.upper()}_NZE_DEMAND",
            ]

            cutoff = datetime.now(UTC) - timedelta(days=self.lookback_days)

            async with self.db.session() as session:
                for series_id in series_candidates:
                    query = (
                        select(DataPointModel.value, DataPointModel.timestamp)
                        .where(
                            and_(
                                DataPointModel.series_id == series_id,
                                DataPointModel.timestamp >= cutoff,
                                DataPointModel.value.isnot(None),
                                DataPointModel.value > 0,
                            )
                        )
                        .order_by(DataPointModel.timestamp)
                    )
                    result = await session.execute(query)
                    rows = result.all()

                    if len(rows) < 3:
                        continue

                    values = np.array([float(r[0]) for r in rows])
                    mean = np.mean(values)
                    std = np.std(values)

                    if mean <= 0:
                        continue

                    cv = float(std / mean)

                    # Normalize to 0-100 (CV of 1.0 = 100% volatility → score 100)
                    score = min(100.0, cv * 100.0)
                    return round(score, 1)

            logger.debug("No price/value data for volatility", mineral=mineral)
            return None

        except Exception as e:
            logger.error("Failed to calculate price volatility", mineral=mineral, error=str(e))
            return None

    async def composite_risk_score(self, mineral: str) -> MineralRiskResult | None:
        """Calculate composite risk score for a mineral.

        Args:
            mineral: Mineral identifier

        Returns:
            MineralRiskResult with full analysis, or None if insufficient data
        """
        if mineral not in self.minerals:
            return None

        import_dependency = await self.calculate_import_dependency(mineral)
        concentration_score, top_suppliers = await self.calculate_concentration_risk(mineral)
        geopolitical_score = await self.calculate_geopolitical_risk(mineral)
        volatility_score = await self.calculate_price_volatility(mineral)

        components: dict[str, float] = {}
        available_weights: dict[str, float] = {}

        # Import dependency is always available (from static config)
        import_score = import_dependency * 100
        components["import_dependency"] = import_score
        available_weights["import_dependency"] = self.weights["import_dependency"]

        if concentration_score is not None:
            components["concentration_risk"] = concentration_score
            available_weights["concentration_risk"] = self.weights["concentration_risk"]

        if volatility_score is not None:
            components["price_volatility"] = volatility_score
            available_weights["price_volatility"] = self.weights["price_volatility"]

        if geopolitical_score is not None:
            components["geopolitical_risk"] = geopolitical_score
            available_weights["geopolitical_risk"] = self.weights["geopolitical_risk"]

        if len(components) < 1:
            logger.warning("Insufficient data for mineral risk calculation", mineral=mineral)
            return None

        composite = self.weighted_composite(components, available_weights)

        if composite >= self.critical_threshold:
            alert_level = "critical"
        elif composite >= self.alert_threshold:
            alert_level = "elevated"
        else:
            alert_level = "normal"

        return MineralRiskResult(
            mineral=mineral,
            composite_risk_score=composite,
            import_dependency=import_dependency,
            concentration_risk=concentration_score if concentration_score is not None else 0.0,
            price_volatility=volatility_score if volatility_score is not None else 0.0,
            geopolitical_risk=geopolitical_score if geopolitical_score is not None else 0.0,
            top_suppliers=top_suppliers,
            alert_level=alert_level,
        )

    async def calculate_all_minerals(self) -> dict[str, MineralRiskResult]:
        """Calculate risk scores for all tracked minerals.

        Returns:
            Dictionary of mineral to MineralRiskResult
        """
        results = {}

        for mineral in self.minerals:
            result = await self.composite_risk_score(mineral)
            if result:
                results[mineral] = result

        return results

    async def calculate_aggregate_risk(self) -> dict[str, Any]:
        """Calculate aggregate critical minerals risk index.

        Returns:
            Aggregate risk summary
        """
        mineral_results = await self.calculate_all_minerals()

        if not mineral_results:
            logger.warning("No mineral risk data available for aggregation")
            return {
                "aggregate_score": None,
                "alert_level": "unknown",
                "minerals_analyzed": 0,
                "data_unavailable": True,
            }

        weighted_sum = 0.0
        weight_total = 0.0

        for mineral, result in mineral_results.items():
            weight = self.minerals[mineral]["weight"]
            weighted_sum += result.composite_risk_score * weight
            weight_total += weight

        aggregate = weighted_sum / weight_total if weight_total > 0 else 50.0

        high_risk = [m for m, r in mineral_results.items() if r.alert_level == "critical"]
        elevated = [m for m, r in mineral_results.items() if r.alert_level == "elevated"]

        if len(high_risk) >= 2 or aggregate >= 80:
            alert_level = "critical"
        elif len(elevated) >= 2 or aggregate >= 60:
            alert_level = "elevated"
        else:
            alert_level = "normal"

        return {
            "aggregate_score": aggregate,
            "alert_level": alert_level,
            "minerals_analyzed": len(mineral_results),
            "high_risk_minerals": high_risk,
            "elevated_risk_minerals": elevated,
            "mineral_scores": {
                m: r.composite_risk_score for m, r in mineral_results.items()
            },
        }

    async def calculate_score(self) -> MonitorResult:
        """Calculate the monitor's composite score.

        Returns:
            MonitorResult with aggregate risk score
        """
        aggregate = await self.calculate_aggregate_risk()
        mineral_results = await self.calculate_all_minerals()

        high_risk = aggregate.get("high_risk_minerals", [])
        elevated = aggregate.get("elevated_risk_minerals", [])

        if high_risk:
            description = f"Critical supply risk for: {', '.join(high_risk)}"
        elif elevated:
            description = f"Elevated supply risk for: {', '.join(elevated)}"
        else:
            description = "Critical mineral supply risks within acceptable range"

        data_completeness = aggregate["minerals_analyzed"] / len(self.minerals)
        confidence = self.calculate_confidence(
            data_completeness=data_completeness,
            sample_size=aggregate["minerals_analyzed"] * 10,
            min_samples=len(self.minerals) * 5,
        )

        return MonitorResult(
            score=aggregate["aggregate_score"],
            severity=self.score_to_severity(aggregate["aggregate_score"]),
            confidence=confidence,
            components=aggregate.get("mineral_scores", {}),
            evidence={
                "alert_level": aggregate["alert_level"],
                "high_risk_minerals": high_risk,
                "elevated_risk_minerals": elevated,
                "mineral_details": {
                    m: r.to_dict() for m, r in mineral_results.items()
                },
            },
            description=description,
        )

    async def generate_signals(self) -> list[BottleneckSignalData]:
        """Generate bottleneck signals for critical mineral risks.

        Returns:
            List of BottleneckSignalData
        """
        mineral_results = await self.calculate_all_minerals()
        signals = []

        for mineral, result in mineral_results.items():
            if result.composite_risk_score < self.alert_threshold:
                continue

            signal = BottleneckSignalData(
                category=BottleneckCategory.SUPPLY_DISRUPTION,
                subcategory=f"critical_mineral_{mineral}",
                severity=min(1.0, result.composite_risk_score / 100),
                confidence=0.75,
                affected_sectors=["MANUFACTURING", "ENERGY", "TECHNOLOGY", "AUTOMOTIVE"],
                affected_commodities=[mineral],
                evidence={
                    "composite_score": result.composite_risk_score,
                    "import_dependency": result.import_dependency,
                    "concentration_risk": result.concentration_risk,
                    "geopolitical_risk": result.geopolitical_risk,
                    "top_suppliers": result.top_suppliers,
                },
                description=(
                    f"Critical mineral supply risk for {self.minerals[mineral]['name']}: "
                    f"Score {result.composite_risk_score:.0f}/100, "
                    f"Import dependency {result.import_dependency:.0%}, "
                    f"Geopolitical risk {result.geopolitical_risk:.0f}/100"
                ),
            )
            signals.append(signal)

        return signals

    async def get_china_exposure_summary(self) -> dict[str, Any]:
        """Get summary of China exposure across all minerals.

        Returns:
            China exposure summary
        """
        china_exposure = {}

        for mineral in self.minerals:
            suppliers = await self._get_supplier_shares(mineral)
            if not suppliers:
                logger.warning("No supplier data for China exposure", mineral=mineral)
                continue
            china_share = suppliers.get("CHN", 0.0)
            china_exposure[mineral] = {
                "direct_share": china_share,
                "risk_level": "high" if china_share > 0.50 else (
                    "moderate" if china_share > 0.20 else "low"
                ),
            }

        # Aggregate China exposure
        if not china_exposure:
            logger.warning("No China exposure data available")
            return {
                "average_china_exposure": None,
                "high_exposure_minerals": [],
                "mineral_details": {},
                "data_unavailable": True,
            }

        avg_exposure = sum(v["direct_share"] for v in china_exposure.values()) / len(china_exposure)

        return {
            "average_china_exposure": avg_exposure,
            "high_exposure_minerals": [
                m for m, v in china_exposure.items() if v["risk_level"] == "high"
            ],
            "mineral_details": china_exposure,
        }
