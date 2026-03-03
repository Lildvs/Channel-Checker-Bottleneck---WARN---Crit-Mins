"""Bottleneck detection engine for identifying economic bottlenecks."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import structlog

from src.analysis.anomaly_detection import AnomalyDetector, DetectedAnomaly
from src.analysis.signals import (
    AnomalyData,
    BottleneckCategory,
    BottleneckSignalData,
    CATEGORY_SERIES_MAP,
    DETECTION_THRESHOLDS,
)
from src.storage.timescale import TimescaleDB

logger = structlog.get_logger()


class BottleneckDetector:
    """Detects economic bottlenecks from time series data."""

    def __init__(self, db: TimescaleDB | None = None):
        """Initialize bottleneck detector.

        Args:
            db: Database connection for fetching data
        """
        self.db = db
        self.anomaly_detector = AnomalyDetector()
        self.logger = logger.bind(component="bottleneck_detector")

    # Quarterly / low-frequency series that need a deep lookback (20 years)
    # to capture enough data points for trend analysis. Daily/weekly series
    # use the standard lookback to keep data volume manageable.
    QUARTERLY_SERIES: frozenset[str] = frozenset({
        "A091RC1Q027SBEA",  # Federal Interest Payments (quarterly)
        "W006RC1Q027SBEA",  # Federal Tax Receipts (quarterly)
        "GFDEBTN",  # Total Public Debt (quarterly)
        "BUSINV",  # Business Inventories (monthly, sparse)
        "AMTMNO",  # Manufacturers Inventories (monthly, sparse)
        "PCE",  # Personal Consumption (monthly)
        "DRCCLACBS",  # Delinquency Rate on Credit Card Loans (quarterly)
        "DRCLACBS",  # Delinquency Rate on Consumer Loans (quarterly)
        "DRSFRMACBS",  # Delinquency Rate on Student Loans (quarterly)
        "SLOAS",  # Student Loans Outstanding (quarterly)
        "ISRATIO",  # Total Business Inventory-to-Sales Ratio (monthly, sparse)
    })

    async def detect_all(
        self,
        lookback_days: int = 400,
    ) -> list[BottleneckSignalData]:
        """Run all bottleneck detection algorithms.

        Pre-fetches all unique series once, then runs all category
        detectors in parallel for maximum throughput.

        Uses a two-tier lookback strategy:
        - Daily/weekly series: ``lookback_days`` (default 400)
        - Quarterly series: 7300 days (20 years) for deep trend analysis

        Args:
            lookback_days: Number of days to look back for high-frequency series

        Returns:
            List of detected bottleneck signals
        """
        import asyncio

        all_series_ids: set[str] = set()
        for series_list in CATEGORY_SERIES_MAP.values():
            all_series_ids.update(series_list)

        now = datetime.now(UTC)
        start_standard = now - timedelta(days=lookback_days)
        start_quarterly = now - timedelta(days=5475)  # 15 years

        prefetched: dict[str, pd.Series] = {}
        if self.db and all_series_ids:

            async def _fetch(sid: str) -> tuple[str, pd.Series | None]:
                start = start_quarterly if sid in self.QUARTERLY_SERIES else start_standard
                try:
                    data = await self.db.get_series_data(sid, start_date=start)
                    if data:
                        df = pd.DataFrame(data)
                        df["timestamp"] = pd.to_datetime(df["timestamp"])
                        df = df.drop_duplicates(subset="timestamp", keep="first")
                        df.set_index("timestamp", inplace=True)
                        df.sort_index(inplace=True)
                        return sid, df["value"]
                except Exception as e:
                    self.logger.warning("Failed to fetch series", series_id=sid, error=str(e))
                return sid, None

            batch_size = 8
            sorted_ids = sorted(all_series_ids)
            for i in range(0, len(sorted_ids), batch_size):
                batch = sorted_ids[i : i + batch_size]
                results = await asyncio.gather(*[_fetch(sid) for sid in batch])
                for sid, series in results:
                    if series is not None:
                        prefetched[sid] = series

        async def _detect_one(category: BottleneckCategory) -> list[BottleneckSignalData]:
            try:
                return await self._detect_category(
                    category, lookback_days, prefetched_data=prefetched,
                )
            except Exception as e:
                self.logger.error(
                    "Failed to detect category",
                    category=category.value,
                    error=str(e),
                )
                return []

        category_results = await asyncio.gather(
            *[_detect_one(cat) for cat in BottleneckCategory]
        )

        all_signals: list[BottleneckSignalData] = []
        for signals in category_results:
            all_signals.extend(signals)

        all_signals.sort(key=lambda s: s.severity, reverse=True)

        self.logger.info(
            "Bottleneck detection complete",
            total_signals=len(all_signals),
            by_category={
                cat.value: len([s for s in all_signals if s.category == cat])
                for cat in BottleneckCategory
            },
        )

        return all_signals

    async def _detect_category(
        self,
        category: BottleneckCategory,
        lookback_days: int,
        prefetched_data: dict[str, pd.Series] | None = None,
    ) -> list[BottleneckSignalData]:
        """Detect bottlenecks for a specific category.

        Args:
            category: Bottleneck category to detect
            lookback_days: Days to look back
            prefetched_data: Optional pre-fetched series data (avoids redundant DB queries)

        Returns:
            List of detected signals
        """
        series_ids = CATEGORY_SERIES_MAP.get(category, [])
        if not series_ids:
            return []

        thresholds = DETECTION_THRESHOLDS.get(category, {})
        signals: list[BottleneckSignalData] = []

        series_data: dict[str, pd.Series] = {}

        if prefetched_data is not None:
            # Use pre-fetched data -- no DB queries needed
            for sid in series_ids:
                if sid in prefetched_data:
                    series_data[sid] = prefetched_data[sid]
        elif self.db:
            # Fallback: fetch individually (for standalone category detection)
            import asyncio
            start_date = datetime.now(UTC) - timedelta(days=lookback_days)

            async def fetch_series(sid: str) -> tuple[str, pd.Series | None]:
                try:
                    data = await self.db.get_series_data(sid, start_date=start_date)
                    if data:
                        df = pd.DataFrame(data)
                        df["timestamp"] = pd.to_datetime(df["timestamp"])
                        df = df.drop_duplicates(subset="timestamp", keep="first")
                        df.set_index("timestamp", inplace=True)
                        df.sort_index(inplace=True)
                        return sid, df["value"]
                except Exception as e:
                    self.logger.warning(
                        "Failed to fetch series",
                        series_id=sid,
                        error=str(e),
                    )
                return sid, None

            results = await asyncio.gather(*[fetch_series(sid) for sid in series_ids])
            for sid, series in results:
                if series is not None:
                    series_data[sid] = series

        if not series_data:
            return []

        all_anomalies = await self.anomaly_detector.detect_multi_series(series_data)

        if category == BottleneckCategory.INVENTORY_SQUEEZE:
            signals.extend(
                await self._detect_inventory_squeeze(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.PRICE_SPIKE:
            signals.extend(
                await self._detect_price_spike(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.LABOR_TIGHTNESS:
            signals.extend(
                await self._detect_labor_tightness(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.CAPACITY_CEILING:
            signals.extend(
                await self._detect_capacity_ceiling(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.DEMAND_SURGE:
            signals.extend(
                await self._detect_demand_surge(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.ENERGY_CRUNCH:
            signals.extend(
                await self._detect_energy_crunch(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.CREDIT_TIGHTENING:
            signals.extend(
                await self._detect_credit_tightening(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.SENTIMENT_SHIFT:
            signals.extend(
                await self._detect_sentiment_shift(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.SHIPPING_CONGESTION:
            signals.extend(
                await self._detect_shipping_congestion(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.SUPPLY_DISRUPTION:
            signals.extend(
                await self._detect_supply_disruption(series_data, all_anomalies, thresholds)
            )
        elif category == BottleneckCategory.FISCAL_DOMINANCE:
            signals.extend(
                await self._detect_fiscal_dominance(series_data, all_anomalies, thresholds)
            )
        else:
            signals.extend(
                await self._detect_generic(category, series_data, all_anomalies, thresholds)
            )

        return signals

    async def _detect_inventory_squeeze(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect inventory squeeze via Pattern C: Sector Signals + Aggregate Overlay.

        Produces BOTH granular sector-specific signals AND a systemic overlay:
        1. Sector signals: Inventory-to-Sales ratio changes for Retail, Mfg, and Total
        2. Aggregate overlay: When 2+ sectors are simultaneously stressed
        3. Orders-vs-inventory divergence: Rising orders + falling inventory = squeeze
        """
        signals: list[BottleneckSignalData] = []
        min_confidence = thresholds.get("min_confidence", 0.6)

        sector_defs = {
            "retail": {"ratio": "RETAILIRSA", "inventory": "RETAILIMSA"},
            "manufacturing": {"ratio": "MNFCTRIRSA", "inventory": "AMTMNO"},
            "total_business": {"ratio": "ISRATIO", "inventory": "BUSINV"},
        }

        sector_stress: dict[str, dict[str, float]] = {}

        for sector, sids in sector_defs.items():
            ratio_series = series_data.get(sids["ratio"])
            inv_series = series_data.get(sids["inventory"])

            if ratio_series is not None and not ratio_series.empty and len(ratio_series) >= 6:
                current_ratio = float(ratio_series.iloc[-1])
                rolling_window = min(24, len(ratio_series))
                hist_mean = float(
                    ratio_series.rolling(rolling_window, min_periods=12).mean().iloc[-1]
                )
                hist_std = float(
                    ratio_series.rolling(rolling_window, min_periods=12).std().iloc[-1]
                ) if len(ratio_series) > 12 else 0.01

                z = (current_ratio - hist_mean) / hist_std if hist_std > 0 else 0.0

                six_mo_ago = float(ratio_series.iloc[-min(6, len(ratio_series))])
                trend = (current_ratio - six_mo_ago) / six_mo_ago if six_mo_ago > 0 else 0.0

                ratio_stress = 0.0
                if z < -1.0:
                    ratio_stress = min(1.0, abs(z) / 3.0)
                if trend < -0.05:
                    ratio_stress = max(ratio_stress, min(1.0, abs(trend) / 0.15))

                if ratio_stress > 0.1:
                    sector_stress[sector] = {
                        "score": ratio_stress,
                        "ratio_current": current_ratio,
                        "ratio_z": z,
                        "ratio_trend_6m": trend,
                    }

                    signals.append(
                        BottleneckSignalData(
                            category=BottleneckCategory.INVENTORY_SQUEEZE,
                            subcategory=f"{sector}_inventory_squeeze",
                            severity=min(1.0, ratio_stress),
                            confidence=min_confidence + 0.05,
                            affected_sectors=[sector.upper().replace("_", " ")],
                            source_series=[sids["ratio"]],
                            evidence={
                                "sector": sector,
                                "ratio_current": round(current_ratio, 3),
                                "ratio_z_score": round(z, 2),
                                "ratio_6m_trend": round(trend, 3),
                            },
                            description=(
                                f"{sector.replace('_', ' ').title()} inventory-to-sales ratio "
                                f"z={z:.1f}, 6m trend={trend:.1%}"
                            ),
                        )
                    )

        if len(sector_stress) >= 2:
            avg_stress = sum(s["score"] for s in sector_stress.values()) / len(sector_stress)
            breadth = len(sector_stress) / len(sector_defs)
            systemic_severity = min(1.0, avg_stress * (1.0 + breadth * 0.3))

            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.INVENTORY_SQUEEZE,
                    subcategory="systemic_inventory_squeeze",
                    severity=systemic_severity,
                    confidence=min(0.95, min_confidence + len(sector_stress) * 0.08),
                    affected_sectors=["MANUFACTURING", "CONSUMER", "RETAIL"],
                    source_series=[
                        sid
                        for defs in sector_defs.values()
                        for sid in defs.values()
                        if sid in series_data
                    ],
                    evidence={
                        "stressed_sectors": list(sector_stress.keys()),
                        "sector_scores": {
                            k: round(v["score"], 3) for k, v in sector_stress.items()
                        },
                        "breadth_ratio": round(breadth, 2),
                        "avg_stress": round(avg_stress, 3),
                    },
                    description=(
                        f"Systemic inventory squeeze: {len(sector_stress)} sectors "
                        f"({', '.join(sector_stress.keys())}), avg stress {avg_stress:.0%}"
                    ),
                )
            )

        orders = series_data.get("DGORDER")
        if orders is None:
            orders = series_data.get("NEWORDER")
        total_inv = series_data.get("BUSINV")
        if (orders is not None and total_inv is not None
                and not orders.empty and not total_inv.empty
                and len(orders) >= 6 and len(total_inv) >= 6):
            orders_trend = (float(orders.iloc[-1]) - float(orders.iloc[-6])) / float(orders.iloc[-6]) if float(orders.iloc[-6]) > 0 else 0.0
            inv_trend = (float(total_inv.iloc[-1]) - float(total_inv.iloc[-6])) / float(total_inv.iloc[-6]) if float(total_inv.iloc[-6]) > 0 else 0.0

            if orders_trend > 0.02 and inv_trend < -0.01:
                divergence_score = min(1.0, (orders_trend - inv_trend) / 0.10)
                signals.append(
                    BottleneckSignalData(
                        category=BottleneckCategory.INVENTORY_SQUEEZE,
                        subcategory="orders_inventory_divergence",
                        severity=divergence_score,
                        confidence=min_confidence + 0.10,
                        affected_sectors=["MANUFACTURING"],
                        source_series=["DGORDER", "BUSINV"],
                        evidence={
                            "orders_6m_trend_pct": round(orders_trend * 100, 1),
                            "inventory_6m_trend_pct": round(inv_trend * 100, 1),
                            "divergence": round((orders_trend - inv_trend) * 100, 1),
                        },
                        description=(
                            f"Orders-inventory divergence: orders +{orders_trend:.1%}, "
                            f"inventory {inv_trend:.1%}"
                        ),
                    )
                )

        return signals

    async def _detect_price_spike(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect price spike via Pattern B: Loudest Signal + Breadth Amplifier.

        Instead of fusing all price series into one number, this pattern:
        1. Finds the LOUDEST individual price signal (highest z-score/momentum)
        2. Amplifies severity based on how many other price series also spiking
        3. Reports the loudest signal as primary, breadth as evidence

        Series: CPIAUCSL, PPIACO, DCOILWTICO, DHHNGSP, GASREGW
        """
        signals: list[BottleneckSignalData] = []
        z_threshold = thresholds.get("z_score_threshold", 2.5)
        min_confidence = thresholds.get("min_confidence", 0.7)
        pct_thresh = thresholds.get("pct_change_threshold", 0.10)

        INDEX_SERIES = frozenset({"CPIAUCSL", "PPIACO"})

        per_series_scores: dict[str, dict[str, float]] = {}

        for series_id, series in series_data.items():
            if series is None or series.empty or len(series) < 5:
                continue

            current = float(series.iloc[-1])

            if series_id in INDEX_SERIES:
                # Index series (CPI, PPI): z-score the YoY *rate of change*,
                # not the level.  The level always rises; what matters is
                # whether the rate of increase is abnormally fast.
                if len(series) >= 13:
                    yoy_changes = series.pct_change(12).dropna()
                    if len(yoy_changes) >= 2:
                        change_mean = float(yoy_changes.mean())
                        change_std = float(yoy_changes.std()) if len(yoy_changes) > 1 else 0.01
                        current_yoy = float(yoy_changes.iloc[-1])
                        z = (current_yoy - change_mean) / change_std if change_std > 0 else 0.0
                        pct_change = current_yoy
                    else:
                        continue
                else:
                    continue
            else:
                # Commodity series: rolling 12-month z-score to avoid regime
                # distortion from structural price shifts.
                rolling_window = min(252, len(series))
                rolling_mean = float(
                    series.rolling(rolling_window, min_periods=10).mean().iloc[-1]
                )
                rolling_std = float(
                    series.rolling(rolling_window, min_periods=10).std().iloc[-1]
                ) if len(series) > 10 else 1.0
                z = (current - rolling_mean) / rolling_std if rolling_std > 0 else 0.0

                lookback = min(30, len(series) - 1)
                prior = float(series.iloc[-lookback - 1])
                pct_change = (current - prior) / prior if prior > 0 else 0.0

            if z >= z_threshold * 0.6 or pct_change >= pct_thresh * 0.5:
                per_series_scores[series_id] = {
                    "z_score": z,
                    "pct_change": pct_change,
                    "score": min(1.0, max(z / z_threshold, pct_change / pct_thresh)),
                    "current": current,
                }

        if not per_series_scores:
            return signals

        loudest_id = max(per_series_scores, key=lambda k: per_series_scores[k]["score"])
        loudest = per_series_scores[loudest_id]
        base_severity = loudest["score"]

        elevated_count = sum(
            1 for sid, info in per_series_scores.items()
            if sid != loudest_id and info["score"] > 0.3
        )
        total_checked = len(series_data)
        breadth_ratio = elevated_count / max(1, total_checked - 1) if total_checked > 1 else 0.0

        breadth_amplifier = 1.0 + (breadth_ratio * 0.5)
        final_severity = min(1.0, base_severity * breadth_amplifier)

        confidence = min(0.95, min_confidence + elevated_count * 0.05)

        evidence = {
            "loudest_series": loudest_id,
            "loudest_z_score": round(loudest["z_score"], 2),
            "loudest_pct_change": round(loudest["pct_change"], 4),
            "loudest_current_value": round(loudest["current"], 2),
            "breadth_count": elevated_count,
            "breadth_ratio": round(breadth_ratio, 2),
            "breadth_amplifier": round(breadth_amplifier, 2),
            "all_series_scores": {
                k: round(v["score"], 3) for k, v in per_series_scores.items()
            },
        }

        if final_severity > 0.15:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.PRICE_SPIKE,
                    subcategory="loudest_signal_with_breadth",
                    severity=final_severity,
                    confidence=confidence,
                    affected_sectors=["CONSUMER", "MANUFACTURING", "ENERGY"],
                    source_series=list(per_series_scores.keys()),
                    evidence=evidence,
                    description=(
                        f"Price spike: {loudest_id} z={loudest['z_score']:.1f} "
                        f"(+{elevated_count} co-spiking), breadth {breadth_ratio:.0%}"
                    ),
                )
            )

        return signals

    async def _detect_labor_tightness(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect labor market tightness via multi-source fusion.

        Combines five scored components into a single composite signal:
          1. Vacancy-Unemployment Ratio (Beveridge Curve)  -- structural tightness (30%)
          2. KC Fed LMCI Level + Momentum                  -- composite conditions (25%)
          3. Hires-to-Separations Flow Balance              -- employment flow   (20%)
          4. Quits Rate                                     -- worker confidence (15%)
          5. ADP Private Employment Momentum                -- real-time signal  (10%)

        WARN notices are used as a forward-looking modifier on the V/U ratio:
        they represent imminent additions to the labor pool (the denominator of
        the V/U ratio) before BLS UNEMPLOY data catches up (60-day WARN lead).

        All FRED series report percentages as whole numbers (2.0 = 2.0%).
        The KC Fed LMCI is centered at 0 (positive = above long-run average).
        ADP employment is in persons; we compute month-over-month growth rate.
        """
        signals: list[BottleneckSignalData] = []

        job_openings = series_data.get("JTSJOL")
        unemploy_level = series_data.get("UNEMPLOY")
        unemployment_rate = series_data.get("UNRATE")
        quits_rate = series_data.get("JTSQUR")
        hires_rate = series_data.get("JTSHIR")
        separations_rate = series_data.get("JTSTSR")
        lmci_activity = series_data.get("FRBKCLMCILA")
        lmci_momentum = series_data.get("FRBKCLMCIM")
        adp_employment = series_data.get("ADPWNUSNERSA")
        payrolls = series_data.get("PAYEMS")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []

        # WARN notices have a 60-90 day lead time.  We split them into:
        #   realized:  effective_date has passed -- these workers are already
        #              in the labor pool but BLS UNEMPLOY may not reflect them
        #              yet.  Added to the V/U denominator.
        #   pending:   effective_date is still in the future -- these workers
        #              are still employed.  NOT added to UNEMPLOY.  Instead
        #              used to reduce *confidence* that current tightness will
        #              persist (a forward-looking risk signal).
        warn_realized: int = 0
        warn_pending: int = 0
        warn_evidence: dict[str, object] = {}
        if self.db is not None:
            try:
                warn_impact = await self.db.get_warn_labor_pool_impact(lookback_months=6)
                if warn_impact:
                    warn_realized = warn_impact.get("realized_employees", 0)
                    warn_pending = warn_impact.get("pending_employees", 0)
                    warn_evidence["warn_realized_employees"] = warn_realized
                    warn_evidence["warn_pending_employees"] = warn_pending
                    warn_evidence["warn_realized_notices"] = warn_impact.get("realized_notices", 0)
                    warn_evidence["warn_pending_notices"] = warn_impact.get("pending_notices", 0)
                    warn_evidence["warn_total_notices"] = warn_impact.get("total_notices", 0)
                    warn_evidence["warn_total_employees"] = warn_impact.get("total_employees", 0)
            except Exception as e:
                self.logger.warning("Failed to fetch WARN data for labor pool adjustment", error=str(e))

        # Weight: 30% -- structural tightness measure
        # JTSJOL / UNEMPLOY (both in thousands, ratio is dimensionless)
        # Historical: 0.2 (deep recession) -> 1.0 (balanced) -> 2.0 (Great Resignation peak)
        #
        # WARN realized adjustment: only workers whose effective_date has
        # already passed are added to the denominator.  BLS UNEMPLOY typically
        # lags 1-2 months; these workers are genuinely in the labor pool but
        # may not yet appear in FRED data.
        vu_ratio: float | None = None
        if (job_openings is not None and unemploy_level is not None
                and not job_openings.empty and not unemploy_level.empty):
            latest_openings = float(job_openings.iloc[-1])
            latest_unemployed = float(unemploy_level.iloc[-1])

            warn_realized_thousands = warn_realized / 1000.0
            adjusted_unemployed = latest_unemployed + warn_realized_thousands

            if adjusted_unemployed > 0:
                vu_ratio = latest_openings / adjusted_unemployed
            else:
                vu_ratio = 0.0

            vu_threshold = thresholds.get("job_openings_ratio", 1.2)
            if vu_ratio > vu_threshold:
                component_scores["vu_ratio"] = min(1.0, (vu_ratio - vu_threshold) / vu_threshold)
            else:
                component_scores["vu_ratio"] = 0.0

            evidence["vu_ratio"] = round(vu_ratio, 3)
            evidence["vu_threshold"] = vu_threshold
            evidence["job_openings_thousands"] = round(latest_openings, 1)
            evidence["unemployed_thousands"] = round(latest_unemployed, 1)
            if warn_realized_thousands > 0:
                unadjusted = latest_openings / latest_unemployed if latest_unemployed > 0 else 0.0
                evidence["vu_ratio_unadjusted"] = round(unadjusted, 3)
                evidence["warn_realized_adjustment_thousands"] = round(warn_realized_thousands, 1)
                evidence["adjusted_unemployed_thousands"] = round(adjusted_unemployed, 1)
                source_series.append("WARN_NOTICES")
            if unemployment_rate is not None and not unemployment_rate.empty:
                evidence["unemployment_rate_pct"] = round(float(unemployment_rate.iloc[-1]), 1)
            source_series.extend(["JTSJOL", "UNEMPLOY"])

        # Weight: 25% -- composite of 24 labor indicators from the KC Fed
        # Level: 0 = long-run average; >0 = above average; <0 = below average
        # Momentum: positive = improving, negative = deteriorating
        if lmci_activity is not None and not lmci_activity.empty:
            activity_val = float(lmci_activity.iloc[-1])
            activity_threshold = thresholds.get("lmci_activity_threshold", 0.5)

            # Tightness score from activity level
            activity_score = min(1.0, max(0.0, activity_val / 1.5)) if activity_val > 0 else 0.0

            # Momentum boost: positive momentum amplifies tightness signal
            momentum_boost = 0.0
            if lmci_momentum is not None and not lmci_momentum.empty:
                momentum_val = float(lmci_momentum.iloc[-1])
                if momentum_val > 0:
                    momentum_boost = min(0.3, momentum_val / 1.0)
                evidence["lmci_momentum"] = round(momentum_val, 4)
                source_series.append("FRBKCLMCIM")

            component_scores["lmci"] = min(1.0, activity_score + momentum_boost)

            evidence["lmci_activity"] = round(activity_val, 4)
            evidence["lmci_activity_threshold"] = activity_threshold
            source_series.append("FRBKCLMCILA")

        # Weight: 20% -- measures net employment flow direction
        # JTSHIR / JTSTSR: >1 = more hiring than separating = tightening
        # Both reported as percentage by FRED (3.3 = 3.3%)
        if (hires_rate is not None and separations_rate is not None
                and not hires_rate.empty and not separations_rate.empty):
            latest_hires = float(hires_rate.iloc[-1])
            latest_seps = float(separations_rate.iloc[-1])

            if latest_seps > 0:
                hs_ratio = latest_hires / latest_seps
            else:
                hs_ratio = 1.0

            hs_threshold = thresholds.get("hires_sep_ratio_threshold", 1.05)
            if hs_ratio > hs_threshold:
                component_scores["hires_sep_flow"] = min(1.0, (hs_ratio - 1.0) / 0.15)
            else:
                component_scores["hires_sep_flow"] = 0.0

            evidence["hires_rate_pct"] = round(latest_hires, 2)
            evidence["separations_rate_pct"] = round(latest_seps, 2)
            evidence["hires_sep_ratio"] = round(hs_ratio, 3)
            source_series.extend(["JTSHIR", "JTSTSR"])

        # Weight: 15% -- high quits = workers confident they can find better jobs
        # FRED JTSQUR reports as percentage (2.3 = 2.3%)
        # Historical: 1.6% (COVID low) -> 2.2% (normal) -> 3.0% (Great Resignation)
        if quits_rate is not None and not quits_rate.empty:
            latest_quits = float(quits_rate.iloc[-1])
            quits_threshold = thresholds.get("quits_rate_threshold", 2.5)

            if latest_quits > quits_threshold:
                # Score: 0 at threshold (2.5%), 1.0 at 4.0%
                component_scores["quits_rate"] = min(1.0, (latest_quits - quits_threshold) / 1.5)
            else:
                component_scores["quits_rate"] = 0.0

            evidence["quits_rate_pct"] = round(latest_quits, 2)
            evidence["quits_threshold_pct"] = quits_threshold
            source_series.append("JTSQUR")

        # Weight: 10% -- month-over-month private employment growth
        # ADPWNUSNERSA is in persons; we compute MoM growth rate
        # Strong positive growth = employers absorbing workers = tightening
        if adp_employment is not None and not adp_employment.empty and len(adp_employment) >= 2:
            current_emp = float(adp_employment.iloc[-1])
            prev_emp = float(adp_employment.iloc[-2])

            if prev_emp > 0:
                adp_mom_growth = (current_emp - prev_emp) / prev_emp
            else:
                adp_mom_growth = 0.0

            # Positive growth beyond 0.1% MoM = tightening signal
            if adp_mom_growth > 0.001:
                component_scores["adp_momentum"] = min(1.0, adp_mom_growth / 0.005)
            else:
                component_scores["adp_momentum"] = 0.0

            evidence["adp_employment_current"] = round(current_emp, 0)
            evidence["adp_mom_growth_pct"] = round(adp_mom_growth * 100, 3)
            source_series.append("ADPWNUSNERSA")

        evidence.update(warn_evidence)

        if payrolls is not None and not payrolls.empty:
            evidence["nonfarm_payrolls_thousands"] = round(float(payrolls.iloc[-1]), 1)
            source_series.append("PAYEMS")

        if not component_scores:
            return signals

        weights = {
            "vu_ratio": 0.30,        # Beveridge Curve / structural tightness (WARN-adjusted)
            "lmci": 0.25,            # KC Fed composite (24 indicators)
            "hires_sep_flow": 0.20,  # Employment flow balance
            "quits_rate": 0.15,      # Worker confidence / voluntary separations
            "adp_momentum": 0.10,    # Private sector real-time signal
        }

        weighted_sum = sum(component_scores.get(k, 0.0) * w for k, w in weights.items())
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        # Confidence scales with data coverage
        base_confidence = thresholds.get("min_confidence", 0.7)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.04)

        # If there are large WARN filings whose effective_date hasn't hit
        # yet, the current tightness reading is less reliable as a
        # forward predictor.  We scale confidence down proportionally.
        # A pending cohort equal to 1% of the current UNEMPLOY level
        # reduces confidence by ~5 percentage points.
        if warn_pending > 0 and "unemployed_thousands" in evidence:
            unemployed_persons = evidence["unemployed_thousands"] * 1000.0
            if unemployed_persons > 0:
                pending_ratio = warn_pending / unemployed_persons
                discount = min(0.15, pending_ratio * 5.0)
                confidence = max(0.50, confidence - discount)
                evidence["warn_confidence_discount"] = round(discount, 3)
                evidence["warn_pending_to_unemploy_ratio"] = round(pending_ratio, 4)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["component_weights"] = {k: w for k, w in weights.items() if k in component_scores}
        evidence["composite_severity"] = round(composite_severity, 3)
        evidence["components_available"] = len(component_scores)
        evidence["components_total"] = len(weights)

        # Deduplicate source_series while preserving order
        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        # Only emit signal if composite indicates meaningful tightness
        if composite_severity > 0.15:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.LABOR_TIGHTNESS,
                    subcategory="composite_labor_tightness",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["MANUFACTURING", "CONSUMER", "HEALTHCARE"],
                    source_series=unique_sources,
                    evidence=evidence,
                    description=(
                        f"Labor tightness composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)}/{len(weights)} indicators"
                        f"{', V/U=' + str(round(vu_ratio, 2)) + ' (WARN-adj)' if vu_ratio is not None else ''})"
                    ),
                )
            )

        return signals

    async def _detect_capacity_ceiling(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect capacity ceiling via 3-component Pattern A fusion.

        Components:
          1. Total Capacity Utilization (TCU)             -- 35%
          2. Manufacturing Capacity Utilization (MCUMFN)  -- 40%
          3. Industrial Production momentum (INDPRO)      -- 25%
        """
        signals: list[BottleneckSignalData] = []

        tcu = series_data.get("TCU")
        mcumfn = series_data.get("MCUMFN")
        indpro = series_data.get("INDPRO")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []

        tcu_elevated = thresholds.get("tcu_elevated", 80.0)
        tcu_critical = thresholds.get("tcu_critical", 85.0)

        def _util_score(series: pd.Series | None, sid: str) -> float | None:
            if series is None or series.empty:
                return None
            val = float(series.iloc[-1])
            evidence[f"{sid}_current_pct"] = round(val, 1)
            source_series.append(sid)

            if val >= tcu_critical:
                return min(1.0, (val - tcu_critical) / 5.0 + 0.6)
            elif val >= tcu_elevated:
                return (val - tcu_elevated) / (tcu_critical - tcu_elevated) * 0.6
            return 0.0

        tcu_score = _util_score(tcu, "TCU")
        if tcu_score is not None:
            component_scores["total_utilization"] = tcu_score

        mcumfn_score = _util_score(mcumfn, "MCUMFN")
        if mcumfn_score is not None:
            component_scores["mfg_utilization"] = mcumfn_score

        # INDPRO momentum only signals a ceiling if utilization is already
        # elevated.  Rising production with spare capacity is healthy growth.
        utilization_elevated = (
            component_scores.get("total_utilization", 0) > 0
            or component_scores.get("mfg_utilization", 0) > 0
        )
        if indpro is not None and not indpro.empty and len(indpro) >= 6:
            current = float(indpro.iloc[-1])
            six_mo_ago = float(indpro.iloc[-min(6, len(indpro))])
            if six_mo_ago > 0:
                momentum = (current - six_mo_ago) / six_mo_ago
                if momentum > 0.02 and utilization_elevated:
                    component_scores["indpro_momentum"] = min(1.0, momentum / 0.08)
                else:
                    component_scores["indpro_momentum"] = 0.0
                evidence["indpro_6m_momentum_pct"] = round(momentum * 100, 1)
                evidence["indpro_utilization_gated"] = not utilization_elevated
                source_series.append("INDPRO")

        if not component_scores:
            return signals

        weights = {
            "total_utilization": 0.35,
            "mfg_utilization": 0.40,
            "indpro_momentum": 0.25,
        }

        weighted_sum = sum(component_scores.get(k, 0.0) * w for k, w in weights.items())
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        base_confidence = thresholds.get("min_confidence", 0.75)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.05)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["composite_severity"] = round(composite_severity, 3)

        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        if composite_severity > 0.15:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.CAPACITY_CEILING,
                    subcategory="composite_capacity_constraint",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["MANUFACTURING", "ENERGY"],
                    source_series=unique_sources,
                    evidence=evidence,
                    description=(
                        f"Capacity ceiling composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)}/3 indicators)"
                    ),
                )
            )

        return signals

    async def _detect_demand_surge(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect demand surge via Pattern D: Primary Signal + Risk Assessment.

        1. Primary: Detect spending momentum from RSXFS and/or PCE
        2. Qualifiers: Check if surge is credit-fueled (TOTALSL) or
           sentiment-divergent (UMCSENT declining while spending rises)
        3. The qualifiers do NOT determine IF a surge exists -- they
           characterize its fragility/sustainability
        """
        signals: list[BottleneckSignalData] = []

        retail = series_data.get("RSXFS")
        pce = series_data.get("PCE")
        sentiment = series_data.get("UMCSENT")
        consumer_credit = series_data.get("TOTALSL")

        single_surge = thresholds.get("single_series_surge_pct", 0.05)
        dual_surge = thresholds.get("dual_series_surge_pct", 0.03)

        evidence: dict[str, object] = {}
        source_series: list[str] = []

        def _yoy_growth(series: pd.Series | None, sid: str) -> float | None:
            if series is None or series.empty or len(series) < 13:
                return None
            current = float(series.iloc[-1])
            year_ago = float(series.iloc[-12])
            if year_ago == 0:
                return None
            growth = (current - year_ago) / year_ago
            evidence[f"{sid}_yoy_growth_pct"] = round(growth * 100, 1)
            evidence[f"{sid}_current"] = round(current, 1)
            source_series.append(sid)
            return growth

        retail_yoy = _yoy_growth(retail, "RSXFS")
        pce_yoy = _yoy_growth(pce, "PCE")

        surging_series: list[str] = []
        max_growth = 0.0

        for sid, yoy in [("RSXFS", retail_yoy), ("PCE", pce_yoy)]:
            if yoy is None:
                continue
            if yoy > single_surge:
                surging_series.append(sid)
                max_growth = max(max_growth, yoy)
            elif yoy > dual_surge and len(surging_series) > 0:
                surging_series.append(sid)
                max_growth = max(max_growth, yoy)

        if retail_yoy is not None and pce_yoy is not None:
            if retail_yoy > dual_surge and pce_yoy > dual_surge:
                surging_series = list(set(surging_series) | {"RSXFS", "PCE"})
                max_growth = max(retail_yoy, pce_yoy)

        if not surging_series:
            return signals

        base_severity = min(1.0, max_growth / 0.12)

        # Risk Assessment: credit-fueled?
        # Credit-fueled surges are real but fragile -- reduce confidence
        # rather than amplifying severity.
        risk_label = "organic"
        sustainability = "high"
        confidence_penalty = 0.0

        credit_yoy = _yoy_growth(consumer_credit, "TOTALSL")
        credit_high = thresholds.get("credit_growth_high", 0.08)

        if credit_yoy is not None and credit_yoy > credit_high:
            risk_label = "credit_fueled"
            sustainability = "low"
            confidence_penalty += 0.12
            evidence["credit_fueled"] = True
            evidence["credit_yoy_pct"] = round(credit_yoy * 100, 1)

        # Risk Assessment: sentiment divergence?
        # Spending up while sentiment drops = surge likely to reverse.
        div_thresh = thresholds.get("sentiment_divergence_threshold", -0.10)
        if sentiment is not None and not sentiment.empty and len(sentiment) >= 12:
            sent_current = float(sentiment.iloc[-1])
            sent_12m_avg = float(sentiment.rolling(12).mean().iloc[-1])
            if sent_12m_avg > 0:
                sent_deviation = (sent_current - sent_12m_avg) / sent_12m_avg
                if sent_deviation < div_thresh:
                    risk_label = (
                        "credit_fueled_divergent" if risk_label == "credit_fueled"
                        else "sentiment_divergent"
                    )
                    sustainability = "low"
                    confidence_penalty += 0.08
                    evidence["sentiment_divergent"] = True
                    evidence["sentiment_deviation_pct"] = round(sent_deviation * 100, 1)
                source_series.append("UMCSENT")

        evidence["risk_label"] = risk_label
        evidence["sustainability"] = sustainability
        evidence["surging_series"] = surging_series
        evidence["max_yoy_growth_pct"] = round(max_growth * 100, 1)

        confidence = min(
            0.95,
            thresholds.get("min_confidence", 0.65) + len(surging_series) * 0.08
        ) - confidence_penalty
        confidence = max(0.40, confidence)

        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        signals.append(
            BottleneckSignalData(
                category=BottleneckCategory.DEMAND_SURGE,
                subcategory=f"demand_surge_{risk_label}",
                severity=min(1.0, base_severity),
                confidence=confidence,
                affected_sectors=["CONSUMER", "MANUFACTURING", "RETAIL"],
                source_series=unique_sources,
                evidence=evidence,
                description=(
                    f"Demand surge: {max_growth:.1%} YoY "
                    f"({', '.join(surging_series)}), risk: {risk_label}"
                ),
            )
        )

        return signals

    async def _detect_energy_crunch(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect energy crunch via 5-component weighted fusion (Pattern A).

        Components:
          1. Oil Price Stress (DCOILWTICO z-score + momentum)      -- 25%
          2. Natural Gas Stress (DHHNGSP z-score + momentum)       -- 20%
          3. Gasoline Price Stress (GASREGW momentum)               -- 15%
          4. Crude Oil Inventory Stress (WCSSTUS1 vs seasonal)     -- 20%
          5. Refinery Utilization Stress (WPULEUS3 extremes)       -- 20%
        """
        signals: list[BottleneckSignalData] = []

        oil = series_data.get("DCOILWTICO")
        nat_gas = series_data.get("DHHNGSP")
        gasoline = series_data.get("GASREGW")
        crude_stocks = series_data.get("WCSSTUS1")
        gas_stocks = series_data.get("WGTSTUS1")
        refinery_util = series_data.get("WPULEUS3")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []

        price_z_thresh = thresholds.get("price_z_threshold", 2.0)
        momentum_thresh = thresholds.get("price_momentum_threshold", 0.15)

        def _price_stress(series: pd.Series, sid: str) -> float:
            if series is None or series.empty or len(series) < 10:
                return 0.0
            current = float(series.iloc[-1])
            mean = float(series.mean())
            std = float(series.std()) if len(series) > 1 else 1.0
            z = (current - mean) / std if std > 0 else 0.0

            lookback = min(60, len(series) - 1)
            prior = float(series.iloc[-lookback - 1])
            momentum = (current - prior) / prior if prior > 0 else 0.0

            level_score = min(1.0, max(0.0, z / price_z_thresh))
            mom_score = min(1.0, max(0.0, momentum / momentum_thresh))
            score = 0.55 * level_score + 0.45 * mom_score

            evidence[f"{sid}_current"] = round(current, 2)
            evidence[f"{sid}_z_score"] = round(z, 2)
            evidence[f"{sid}_60d_momentum_pct"] = round(momentum * 100, 1)
            source_series.append(sid)
            return score

        oil_score = _price_stress(oil, "DCOILWTICO")
        if oil_score > 0:
            component_scores["oil_price"] = oil_score

        gas_score = _price_stress(nat_gas, "DHHNGSP")
        if gas_score > 0:
            component_scores["nat_gas_price"] = gas_score

        gasoline_score = _price_stress(gasoline, "GASREGW")
        if gasoline_score > 0:
            component_scores["gasoline_price"] = gasoline_score

        stocks_threshold = thresholds.get("stocks_seasonal_threshold", 0.80)
        if crude_stocks is not None and not crude_stocks.empty and len(crude_stocks) > 52:
            current_stocks = float(crude_stocks.iloc[-1])
            seasonal_mean = float(crude_stocks.rolling(52, min_periods=26).mean().iloc[-1])

            if seasonal_mean > 0:
                ratio = current_stocks / seasonal_mean
                if ratio < stocks_threshold:
                    component_scores["crude_inventory"] = min(
                        1.0, (stocks_threshold - ratio) / 0.20
                    )
                else:
                    component_scores["crude_inventory"] = 0.0

                evidence["crude_stocks_current"] = round(current_stocks, 0)
                evidence["crude_stocks_seasonal_avg"] = round(seasonal_mean, 0)
                evidence["crude_stocks_ratio"] = round(ratio, 3)
                source_series.append("WCSSTUS1")

        # High util = capacity strain (always relevant).
        # Low util = only an energy crunch signal if prices are also elevated
        # (supply-side outage).  Low util + low prices = demand weakness, not
        # a crunch.
        refinery_high = thresholds.get("refinery_util_high", 0.92)
        refinery_low = thresholds.get("refinery_util_low", 0.80)
        if refinery_util is not None and not refinery_util.empty:
            util_pct = float(refinery_util.iloc[-1]) / 100.0

            if util_pct > refinery_high:
                component_scores["refinery_util"] = min(1.0, (util_pct - refinery_high) / 0.05)
                evidence["refinery_stress_type"] = "capacity_strain"
            elif util_pct < refinery_low:
                prices_elevated = (
                    component_scores.get("oil_price", 0) > 0.3
                    or component_scores.get("gasoline_price", 0) > 0.3
                )
                if prices_elevated:
                    component_scores["refinery_util"] = min(1.0, (refinery_low - util_pct) / 0.10)
                    evidence["refinery_stress_type"] = "supply_side_outage"
                else:
                    component_scores["refinery_util"] = 0.0
                    evidence["refinery_stress_type"] = "demand_weakness_excluded"
            else:
                component_scores["refinery_util"] = 0.0

            evidence["refinery_utilization_pct"] = round(util_pct * 100, 1)
            source_series.append("WPULEUS3")

        if not component_scores:
            return signals

        weights = {
            "oil_price": 0.25,
            "nat_gas_price": 0.20,
            "gasoline_price": 0.15,
            "crude_inventory": 0.20,
            "refinery_util": 0.20,
        }

        weighted_sum = sum(component_scores.get(k, 0.0) * w for k, w in weights.items())
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        base_confidence = thresholds.get("min_confidence", 0.70)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.04)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["composite_severity"] = round(composite_severity, 3)
        evidence["components_available"] = len(component_scores)

        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        if composite_severity > 0.15:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.ENERGY_CRUNCH,
                    subcategory="composite_energy_stress",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["ENERGY", "TRANSPORTATION", "MANUFACTURING"],
                    source_series=unique_sources,
                    evidence=evidence,
                    description=(
                        f"Energy crunch composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)}/5 indicators)"
                    ),
                )
            )

        return signals

    async def _detect_credit_tightening(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect credit tightening by synthesizing multiple financial stress indicators.

        Combines Fed Funds Rate (DFF), 10Y Treasury (DGS10), yield curve (T10Y2Y),
        and high-yield spread (BAMLH0A0HYM2) into a single composite signal.
        """
        signals: list[BottleneckSignalData] = []

        fed_funds = series_data.get("DFF")
        treasury_10y = series_data.get("DGS10")
        yield_curve = series_data.get("T10Y2Y")
        hy_spread = series_data.get("BAMLH0A0HYM2")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []
        all_anomaly_data: list[AnomalyData] = []

        # A stable rate (even if high) is already priced in; only deviation
        # from the 6-month rolling average signals active tightening.
        if fed_funds is not None and not fed_funds.empty and len(fed_funds) >= 2:
            current_rate = float(fed_funds.iloc[-1])
            rolling_window = min(126, len(fed_funds))
            rolling_avg = float(
                fed_funds.rolling(rolling_window, min_periods=2).mean().iloc[-1]
            )
            deviation = current_rate - rolling_avg

            level_score = min(1.0, max(0.0, deviation / 2.0))

            rate_3m_ago = float(fed_funds.iloc[-min(63, len(fed_funds))]) if len(fed_funds) > 10 else current_rate
            rate_change = current_rate - rate_3m_ago
            momentum_score = min(1.0, max(0.0, rate_change / 1.5))

            component_scores["fed_funds"] = 0.6 * level_score + 0.4 * momentum_score

            evidence["fed_funds_rate"] = current_rate
            evidence["fed_funds_rolling_avg"] = round(rolling_avg, 3)
            evidence["fed_funds_deviation"] = round(deviation, 3)
            evidence["fed_funds_3m_change"] = round(rate_change, 3)
            source_series.append("DFF")

        # Deep inversion flags abnormal conditions but is not itself tightening
        # (long-term borrowing is actually cheaper during inversions).
        if yield_curve is not None and not yield_curve.empty:
            spread = float(yield_curve.iloc[-1])

            if spread < 0:
                component_scores["yield_curve_inversion"] = min(1.0, abs(spread) / 1.5)
            else:
                component_scores["yield_curve_inversion"] = 0.0

            evidence["yield_curve_spread"] = spread
            evidence["yield_curve_inverted"] = spread < 0
            source_series.append("T10Y2Y")

            # Component 2b: Uninversion velocity -- the curve steepening after
            # an inversion is the real recession-arrival signal.
            if len(yield_curve) >= 126:
                trough_6m = float(yield_curve.iloc[-126:].min())
                steepening = spread - trough_6m
                was_inverted_recently = trough_6m < -0.05

                if was_inverted_recently and steepening > 0.50:
                    component_scores["yield_curve_uninversion"] = min(
                        1.0, (steepening - 0.50) / 1.0
                    )
                else:
                    component_scores["yield_curve_uninversion"] = 0.0

                evidence["yield_curve_6m_trough"] = round(trough_6m, 3)
                evidence["yield_curve_steepening_from_trough"] = round(steepening, 3)
                evidence["was_inverted_recently"] = was_inverted_recently

        if hy_spread is not None and not hy_spread.empty and len(hy_spread) >= 2:
            current_spread = float(hy_spread.iloc[-1])
            hist_median = float(hy_spread.median())
            spread_ratio = current_spread / hist_median if hist_median > 0 else 1.0

            # Above median = tightening; 2x median = severe
            component_scores["hy_spread"] = min(1.0, max(0.0, (spread_ratio - 1.0) / 1.0))

            evidence["hy_spread_current"] = round(current_spread, 2)
            evidence["hy_spread_median"] = round(hist_median, 2)
            evidence["hy_spread_ratio"] = round(spread_ratio, 2)
            source_series.append("BAMLH0A0HYM2")

        if treasury_10y is not None and not treasury_10y.empty:
            t10_anomalies = [a for a in anomalies.get("DGS10", []) if abs(a.z_score) >= 1.5]
            if t10_anomalies:
                component_scores["treasury_anomaly"] = max(a.severity for a in t10_anomalies)
                all_anomaly_data.extend(
                    AnomalyData(
                        series_id="DGS10", timestamp=a.timestamp,
                        actual_value=a.value, expected_value=a.expected_value,
                        z_score=a.z_score, detection_method=a.detection_method,
                        anomaly_type=a.anomaly_type,
                    ) for a in t10_anomalies
                )
                source_series.append("DGS10")

        if not component_scores:
            return signals

        weights = {
            "fed_funds": 0.35,
            "yield_curve_inversion": 0.10,
            "yield_curve_uninversion": 0.20,
            "hy_spread": 0.25,
            "treasury_anomaly": 0.10,
        }
        weighted_sum = sum(component_scores.get(k, 0.0) * w for k, w in weights.items())
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        # Confidence increases with more data sources available
        base_confidence = thresholds.get("min_confidence", 0.65)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.05)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["composite_severity"] = round(composite_severity, 3)
        evidence["components_available"] = len(component_scores)

        # Only emit a signal if composite severity indicates meaningful tightening
        if composite_severity > 0.15:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.CREDIT_TIGHTENING,
                    subcategory="composite_credit_stress",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["FINANCE", "CONSUMER", "REAL_ESTATE"],
                    source_series=source_series,
                    anomalies=all_anomaly_data if all_anomaly_data else None,
                    evidence=evidence,
                    description=(
                        f"Credit tightening composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)} indicators)"
                    ),
                )
            )

        return signals

    async def _detect_sentiment_shift(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect sentiment shift via 9-component weighted fusion (Pattern A).

        Components and weights:
          1. Survey -- UMich + OECD Consumer Confidence (combined)   -- 5%
          2. NFIB Small Business Optimism                           -- 12%
          3. Credit Card Delinquency (DRCCLACBS)                    -- 18%
          4. Student Loan Delinquency (DRSFRMACBS)                  -- 20% (5% during policy events)
          5. Personal Savings Rate (PSAVERT)                        -- 12%
          6. NFCI Financial Conditions                              -- 12%
          7. Credit Card Loans Outstanding growth                   -- 6%
          8. Google Trends Stress Index (TRENDS_STRESS_INDEX)       -- 10%
          9. BNPL Delinquency (BNPL_AFFIRM_DELINQUENCY_30PLUS)     -- 5%
        """
        signals: list[BottleneckSignalData] = []

        umich = series_data.get("UMCSENT")
        consumer_conf = series_data.get("CSCICP03USM665S")
        nfci = series_data.get("NFCI")
        cc_delinq = series_data.get("DRCCLACBS")
        consumer_delinq = series_data.get("DRCLACBS")
        student_delinq = series_data.get("DRSFRMACBS")
        cc_outstanding = series_data.get("CCLACBW027SBOG")
        student_outstanding = series_data.get("SLOAS")
        savings_rate = series_data.get("PSAVERT")
        consumer_credit = series_data.get("TOTALSL")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []

        survey_scores: list[float] = []
        for sid, series, label in [
            ("UMCSENT", umich, "umich"),
            ("CSCICP03USM665S", consumer_conf, "consumer_conf"),
        ]:
            if series is not None and not series.empty and len(series) >= 3:
                current = float(series.iloc[-1])
                hist_mean = float(series.mean())
                hist_std = float(series.std()) if len(series) > 1 else 1.0
                z = (current - hist_mean) / hist_std if hist_std > 0 else 0.0

                prev = float(series.iloc[-2])
                mom = (current - prev) / prev if prev != 0 else 0.0

                score = 0.6 * min(1.0, max(0.0, -z / 2.5)) + 0.4 * min(1.0, max(0.0, -mom / 0.10))
                survey_scores.append(score)
                evidence[f"{label}_current"] = round(current, 1)
                evidence[f"{label}_z"] = round(z, 2)
                source_series.append(sid)

        if survey_scores:
            component_scores["survey_sentiment"] = sum(survey_scores) / len(survey_scores)

        # Not in prefetched FRED data; check if available via series_data injection
        nfib_opt = series_data.get("NFIB_OPT_INDEX")
        if nfib_opt is not None and not nfib_opt.empty:
            nfib_val = float(nfib_opt.iloc[-1])
            pessimistic = thresholds.get("nfib_pessimistic", 90)
            recessionary = thresholds.get("nfib_recessionary", 85)

            if nfib_val < recessionary:
                component_scores["nfib_optimism"] = min(1.0, (recessionary - nfib_val) / 10.0 + 0.5)
            elif nfib_val < pessimistic:
                component_scores["nfib_optimism"] = (pessimistic - nfib_val) / (pessimistic - recessionary) * 0.5
            else:
                component_scores["nfib_optimism"] = 0.0

            evidence["nfib_current"] = round(nfib_val, 1)
            source_series.append("NFIB_OPT_INDEX")

        cc_delinq_thresh = thresholds.get("credit_card_delinq_threshold", 3.5)
        if cc_delinq is not None and not cc_delinq.empty:
            val = float(cc_delinq.iloc[-1])
            if val > cc_delinq_thresh:
                component_scores["cc_delinquency"] = min(1.0, (val - cc_delinq_thresh) / 3.0)
            else:
                component_scores["cc_delinquency"] = max(0.0, val / cc_delinq_thresh * 0.3)
            evidence["cc_delinquency_pct"] = round(val, 2)
            source_series.append("DRCCLACBS")

        # Student loan delinquency is heavily driven by federal policy (payment
        # pauses, forbearance).  When a policy event is detected (>50% QoQ
        # change), the weight collapses from 20% to 5% and the freed weight
        # redistributes proportionally to other active components.
        student_loan_policy_event = False
        student_thresh = thresholds.get("student_delinq_threshold", 5.0)
        if student_delinq is not None and not student_delinq.empty:
            val = float(student_delinq.iloc[-1])
            if val > student_thresh:
                component_scores["student_delinquency"] = min(1.0, (val - student_thresh) / 5.0)
            else:
                component_scores["student_delinquency"] = max(0.0, val / student_thresh * 0.2)
            evidence["student_delinquency_pct"] = round(val, 2)
            source_series.append("DRSFRMACBS")

            if len(student_delinq) >= 4:
                quarter_ago = float(student_delinq.iloc[-min(4, len(student_delinq))])
                if quarter_ago > 0:
                    qoq_change = abs(val - quarter_ago) / quarter_ago
                    if qoq_change > 0.50:
                        student_loan_policy_event = True
                        evidence["student_loan_policy_event"] = True
                        evidence["student_loan_qoq_change_pct"] = round(qoq_change * 100, 1)

        savings_critical = thresholds.get("savings_rate_critical", 3.0)
        if savings_rate is not None and not savings_rate.empty:
            val = float(savings_rate.iloc[-1])
            if val < savings_critical:
                component_scores["savings_rate"] = min(1.0, (savings_critical - val) / 3.0)
            elif val < 5.0:
                component_scores["savings_rate"] = max(0.0, (5.0 - val) / 5.0 * 0.3)
            else:
                component_scores["savings_rate"] = 0.0
            evidence["savings_rate_pct"] = round(val, 1)
            source_series.append("PSAVERT")

        nfci_thresh = thresholds.get("nfci_tight_threshold", 0.0)
        if nfci is not None and not nfci.empty:
            val = float(nfci.iloc[-1])
            if val > nfci_thresh:
                component_scores["nfci_conditions"] = min(1.0, val / 1.0)
            else:
                component_scores["nfci_conditions"] = 0.0
            evidence["nfci_current"] = round(val, 3)
            source_series.append("NFCI")

        if cc_outstanding is not None and not cc_outstanding.empty and len(cc_outstanding) >= 52:
            current = float(cc_outstanding.iloc[-1])
            year_ago = float(cc_outstanding.iloc[-min(52, len(cc_outstanding))])
            if year_ago > 0:
                growth = (current - year_ago) / year_ago
                if growth > 0.08:
                    component_scores["credit_growth"] = min(1.0, (growth - 0.08) / 0.12)
                else:
                    component_scores["credit_growth"] = 0.0
                evidence["cc_outstanding_yoy_growth_pct"] = round(growth * 100, 1)
                source_series.append("CCLACBW027SBOG")

        # Component 8: Google Trends Stress Index (10%)
        trends_stress = series_data.get("TRENDS_STRESS_INDEX")
        trends_z_thresh = thresholds.get("trends_stress_z_threshold", 1.5)
        if trends_stress is not None and not trends_stress.empty and len(trends_stress) >= 5:
            val = float(trends_stress.iloc[-1])
            hist_mean = float(trends_stress.mean())
            hist_std = float(trends_stress.std()) if len(trends_stress) > 1 else 1.0
            z = (val - hist_mean) / hist_std if hist_std > 0 else 0.0

            if z > trends_z_thresh:
                component_scores["trends_stress"] = min(1.0, z / (trends_z_thresh * 2))
            else:
                component_scores["trends_stress"] = 0.0
            evidence["trends_stress_index"] = round(val, 1)
            evidence["trends_stress_z"] = round(z, 2)
            source_series.append("TRENDS_STRESS_INDEX")

        bnpl = series_data.get("BNPL_AFFIRM_DELINQUENCY_30PLUS")
        if bnpl is not None and not bnpl.empty:
            val = float(bnpl.iloc[-1])
            if val > 5.0:
                component_scores["bnpl_delinquency"] = min(1.0, (val - 5.0) / 10.0)
            else:
                component_scores["bnpl_delinquency"] = 0.0
            evidence["bnpl_delinquency_pct"] = round(val, 2)
            source_series.append("BNPL_AFFIRM_DELINQUENCY_30PLUS")

        if not component_scores:
            return signals

        base_weights = {
            "survey_sentiment": 0.05,
            "nfib_optimism": 0.12,
            "cc_delinquency": 0.18,
            "student_delinquency": 0.20,
            "savings_rate": 0.12,
            "nfci_conditions": 0.12,
            "credit_growth": 0.06,
            "trends_stress": 0.10,
            "bnpl_delinquency": 0.05,
        }

        # Dynamic weight adjustment: when a student loan policy event is
        # detected, collapse student_delinquency from 20% to 5% and
        # redistribute the freed 15% proportionally across other active
        # components.
        weights = dict(base_weights)
        if student_loan_policy_event and "student_delinquency" in component_scores:
            freed_weight = 0.15
            weights["student_delinquency"] = 0.05

            other_active = {
                k: w for k, w in weights.items()
                if k != "student_delinquency" and k in component_scores
            }
            other_total = sum(other_active.values())
            if other_total > 0:
                for k in other_active:
                    weights[k] += freed_weight * (other_active[k] / other_total)

            evidence["student_loan_weight_adjusted"] = True
            evidence["student_loan_effective_weight"] = 0.05

        weighted_sum = sum(component_scores.get(k, 0.0) * w for k, w in weights.items())
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        base_confidence = thresholds.get("min_confidence", 0.55)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.04)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["component_weights"] = {
            k: round(w, 4) for k, w in weights.items() if k in component_scores
        }
        evidence["composite_severity"] = round(composite_severity, 3)
        evidence["components_available"] = len(component_scores)
        evidence["components_total"] = len(weights)

        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        if composite_severity > 0.10:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.SENTIMENT_SHIFT,
                    subcategory="composite_sentiment_stress",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["CONSUMER", "RETAIL", "FINANCE"],
                    source_series=unique_sources,
                    evidence=evidence,
                    description=(
                        f"Sentiment shift composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)}/{len(weights)} indicators)"
                    ),
                )
            )

        return signals

    async def _detect_shipping_congestion(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect shipping/freight congestion by synthesizing transportation indicators.

        Combines Transportation Services Index (TSIFRGHT) and Rail Freight
        Carloads (RAILFRTCARLOADSD11) into a single composite signal.
        """
        signals: list[BottleneckSignalData] = []

        tsi_freight = series_data.get("TSIFRGHT")
        rail_freight = series_data.get("RAILFRTCARLOADSD11")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []
        all_anomaly_data: list[AnomalyData] = []

        # A drop in the index signals congestion/disruption (shippers can't move goods)
        if tsi_freight is not None and not tsi_freight.empty and len(tsi_freight) >= 3:
            current = float(tsi_freight.iloc[-1])
            hist_mean = float(tsi_freight.mean())
            hist_std = float(tsi_freight.std()) if len(tsi_freight) > 1 else 1.0

            z = (current - hist_mean) / hist_std if hist_std > 0 else 0.0

            # Negative z = below-normal freight activity = potential congestion
            # But also: a sharp spike above normal can mean surging demand overwhelming capacity
            congestion_score = 0.0
            if z < -1.0:
                # Below normal -- freight disruption
                congestion_score = min(1.0, abs(z) / 3.0)
            elif z > 2.0:
                # Way above normal -- demand overwhelming capacity
                congestion_score = min(1.0, (z - 2.0) / 2.0)

            component_scores["tsi_freight"] = congestion_score
            evidence["tsi_current"] = round(current, 1)
            evidence["tsi_z_score"] = round(z, 2)
            source_series.append("TSIFRGHT")

            tsi_anomalies = [a for a in anomalies.get("TSIFRGHT", []) if abs(a.z_score) >= 1.5]
            for a in tsi_anomalies:
                all_anomaly_data.append(AnomalyData(
                    series_id="TSIFRGHT", timestamp=a.timestamp,
                    actual_value=a.value, expected_value=a.expected_value,
                    z_score=a.z_score, detection_method=a.detection_method,
                    anomaly_type=a.anomaly_type,
                ))

        # Sharp drops = supply chain disruption; sharp spikes = capacity strain
        if rail_freight is not None and not rail_freight.empty and len(rail_freight) >= 3:
            current = float(rail_freight.iloc[-1])
            hist_mean = float(rail_freight.mean())
            hist_std = float(rail_freight.std()) if len(rail_freight) > 1 else 1.0

            z = (current - hist_mean) / hist_std if hist_std > 0 else 0.0

            congestion_score = 0.0
            if z < -1.0:
                congestion_score = min(1.0, abs(z) / 3.0)
            elif z > 2.0:
                congestion_score = min(1.0, (z - 2.0) / 2.0)

            component_scores["rail_freight"] = congestion_score
            evidence["rail_current"] = round(current, 1)
            evidence["rail_z_score"] = round(z, 2)
            source_series.append("RAILFRTCARLOADSD11")

            rail_anomalies = [a for a in anomalies.get("RAILFRTCARLOADSD11", []) if abs(a.z_score) >= 1.5]
            for a in rail_anomalies:
                all_anomaly_data.append(AnomalyData(
                    series_id="RAILFRTCARLOADSD11", timestamp=a.timestamp,
                    actual_value=a.value, expected_value=a.expected_value,
                    z_score=a.z_score, detection_method=a.detection_method,
                    anomaly_type=a.anomaly_type,
                ))

        if not component_scores:
            return signals

        # Equal weighting
        composite_severity = sum(component_scores.values()) / len(component_scores)

        base_confidence = thresholds.get("min_confidence", 0.65)
        confidence = min(0.90, base_confidence + len(component_scores) * 0.08)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["composite_severity"] = round(composite_severity, 3)

        if composite_severity > 0.15:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.SHIPPING_CONGESTION,
                    subcategory="composite_freight_stress",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["TRANSPORTATION", "MANUFACTURING", "CONSUMER"],
                    source_series=source_series,
                    anomalies=all_anomaly_data if all_anomaly_data else None,
                    evidence=evidence,
                    description=(
                        f"Shipping congestion composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)} indicators)"
                    ),
                )
            )

        return signals

    async def _detect_supply_disruption(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect supply disruption via 4-component Pattern A fusion.

        Components:
          1. Port Congestion (US composite from port_congestion collector) -- 35%
          2. Shipping Cost / TEU Volumes proxy                            -- 20%
          3. SEC Filing Risk Factor Keywords density                      -- 15%
          4. Philly Fed Delivery Time Diffusion Index (DTCDFNA066MNFRBPHI)-- 30%
        """
        signals: list[BottleneckSignalData] = []

        port_composite = series_data.get("PORT_CONGESTION_US_COMPOSITE")
        teu_volumes = series_data.get("SHIPPING_POLA_TEU_TOTAL")
        sec_risk_10k = series_data.get("SEC_10-K_RISK_FACTOR_KEYWORDS")
        sec_risk_10q = series_data.get("SEC_10-Q_RISK_FACTOR_KEYWORDS")
        delivery_time = series_data.get("DTCDFNA066MNFRBPHI")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []

        port_high = thresholds.get("port_congestion_high_days", 5.0)
        port_critical = thresholds.get("port_congestion_critical_days", 10.0)

        if port_composite is not None and not port_composite.empty:
            val = float(port_composite.iloc[-1])
            if val >= port_critical:
                component_scores["port_congestion"] = min(1.0, (val - port_critical) / 10.0 + 0.6)
            elif val >= port_high:
                component_scores["port_congestion"] = (val - port_high) / (port_critical - port_high) * 0.6
            else:
                component_scores["port_congestion"] = 0.0
            evidence["port_congestion_days"] = round(val, 1)
            source_series.append("PORT_CONGESTION_US_COMPOSITE")

        port_codes = ["USLAX", "USLGB", "USNYC", "USSAV", "USHOU"]
        congested_ports = 0
        for code in port_codes:
            p = series_data.get(f"PORT_CONGESTION_{code}")
            if p is not None and not p.empty and float(p.iloc[-1]) > port_high:
                congested_ports += 1
        if congested_ports > 0:
            evidence["congested_ports_count"] = congested_ports

        if teu_volumes is not None and not teu_volumes.empty and len(teu_volumes) >= 6:
            current = float(teu_volumes.iloc[-1])
            six_ago = float(teu_volumes.iloc[-min(6, len(teu_volumes))])
            if six_ago > 0:
                change = (current - six_ago) / six_ago
                if change > 0.15:
                    component_scores["shipping_cost"] = min(1.0, change / 0.50)
                elif change < -0.15:
                    component_scores["shipping_cost"] = min(1.0, abs(change) / 0.50 * 0.5)
                else:
                    component_scores["shipping_cost"] = 0.0
                evidence["teu_6m_change_pct"] = round(change * 100, 1)
                source_series.append("SHIPPING_POLA_TEU_TOTAL")

        sec_series = sec_risk_10k if sec_risk_10k is not None else sec_risk_10q
        sec_thresh = thresholds.get("sec_signal_density_threshold", 0.15)
        if sec_series is not None and not sec_series.empty:
            val = float(sec_series.iloc[-1])
            if val > sec_thresh:
                component_scores["sec_risk_signals"] = min(1.0, (val - sec_thresh) / 0.30)
            else:
                component_scores["sec_risk_signals"] = 0.0
            evidence["sec_risk_density"] = round(val, 3)
            source_series.append(
                "SEC_10-K_RISK_FACTOR_KEYWORDS" if sec_risk_10k is not None
                else "SEC_10-Q_RISK_FACTOR_KEYWORDS"
            )

        delivery_thresh = thresholds.get("delivery_time_threshold", 10.0)
        if delivery_time is not None and not delivery_time.empty:
            val = float(delivery_time.iloc[-1])
            hist_mean = float(delivery_time.mean())
            hist_std = float(delivery_time.std()) if len(delivery_time) > 1 else 1.0
            z = (val - hist_mean) / hist_std if hist_std > 0 else 0.0

            if val > delivery_thresh:
                component_scores["delivery_times"] = min(1.0, (val - delivery_thresh) / 30.0)
            elif z > 1.5:
                component_scores["delivery_times"] = min(0.5, z / 4.0)
            else:
                component_scores["delivery_times"] = 0.0
            evidence["delivery_time_idx"] = round(val, 1)
            evidence["delivery_time_z"] = round(z, 2)
            source_series.append("DTCDFNA066MNFRBPHI")

        if not component_scores:
            return signals

        weights = {
            "port_congestion": 0.35,
            "shipping_cost": 0.20,
            "sec_risk_signals": 0.15,
            "delivery_times": 0.30,
        }

        weighted_sum = sum(component_scores.get(k, 0.0) * w for k, w in weights.items())
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        base_confidence = thresholds.get("min_confidence", 0.65)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.06)

        evidence["component_scores"] = {k: round(v, 3) for k, v in component_scores.items()}
        evidence["composite_severity"] = round(composite_severity, 3)
        evidence["components_available"] = len(component_scores)

        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        if composite_severity > 0.12:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.SUPPLY_DISRUPTION,
                    subcategory="composite_supply_disruption",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["TRANSPORTATION", "MANUFACTURING", "CONSUMER"],
                    source_series=unique_sources,
                    evidence=evidence,
                    description=(
                        f"Supply disruption composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)}/4 indicators)"
                    ),
                )
            )

        return signals

    async def _detect_fiscal_dominance(
        self,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Detect fiscal dominance conditions via multi-source fusion.

        Core Lildvs thesis: when federal interest payments exceed ~25% of tax
        receipts, the Fed is forced to monetize debt regardless of inflation.

        Components:
          1. Interest-to-Receipts Ratio  (35%) -- primary fiscal dominance signal
          2. TGA Drawdown               (20%) -- Treasury funding stress
          3. Debt Growth Momentum        (15%) -- compounding interest burden
          4. Reverse Repo Drainage       (15%) -- liquidity buffer exhaustion
          5. Fed Balance Sheet Momentum  (15%) -- stealth QE detection
        """
        signals: list[BottleneckSignalData] = []

        interest_payments = series_data.get("A091RC1Q027SBEA")
        tax_receipts = series_data.get("W006RC1Q027SBEA")
        total_debt = series_data.get("GFDEBTN")
        tga_balance = series_data.get("WTREGEN")
        reverse_repo = series_data.get("RRPONTSYD")
        fed_assets = series_data.get("WALCL")
        bank_reserves = series_data.get("WRESBAL")

        component_scores: dict[str, float] = {}
        evidence: dict[str, object] = {}
        source_series: list[str] = []

        # A091RC1Q027SBEA / W006RC1Q027SBEA -- both quarterly SAAR in billions
        # >0.25 = fiscal dominance territory (Lildvs's threshold)
        ir_ratio: float | None = None
        if (interest_payments is not None and tax_receipts is not None
                and not interest_payments.empty and not tax_receipts.empty):
            latest_interest = float(interest_payments.iloc[-1])
            latest_receipts = float(tax_receipts.iloc[-1])

            if latest_receipts > 0:
                ir_ratio = latest_interest / latest_receipts
            else:
                ir_ratio = 0.0

            ir_threshold = thresholds.get("interest_receipts_ratio_threshold", 0.25)
            if ir_ratio > ir_threshold:
                # Score: 0 at threshold, 1.0 at 2x threshold (50%)
                component_scores["interest_receipts"] = min(
                    1.0, (ir_ratio - ir_threshold) / ir_threshold
                )
            else:
                component_scores["interest_receipts"] = 0.0

            evidence["interest_payments_billions"] = round(latest_interest, 1)
            evidence["tax_receipts_billions"] = round(latest_receipts, 1)
            evidence["interest_receipts_ratio"] = round(ir_ratio, 4)
            evidence["ir_threshold"] = ir_threshold
            source_series.extend(["A091RC1Q027SBEA", "W006RC1Q027SBEA"])

        # WTREGEN in millions.  A drawdown only signals fiscal stress when
        # the absolute balance is low (approaching extraordinary-measures
        # territory, ~$100B).  Drawdowns from a healthy balance are normal
        # Treasury cash management, not stress.
        # 26-week lookback smooths out seasonal patterns (e.g. tax refunds).
        tga_stress_floor_millions = 100_000.0
        if tga_balance is not None and not tga_balance.empty and len(tga_balance) >= 2:
            current_tga = float(tga_balance.iloc[-1])
            lookback = min(26, len(tga_balance))
            recent_peak = float(tga_balance.iloc[-lookback:].max())

            if recent_peak > 0:
                tga_drawdown = (current_tga - recent_peak) / recent_peak
            else:
                tga_drawdown = 0.0

            dd_threshold = thresholds.get("tga_drawdown_threshold", -0.20)
            if current_tga < tga_stress_floor_millions and tga_drawdown < dd_threshold:
                component_scores["tga_drawdown"] = min(
                    1.0, abs(tga_drawdown - dd_threshold) / 0.30
                )
            elif tga_drawdown < dd_threshold * 2:
                component_scores["tga_drawdown"] = min(
                    1.0, abs(tga_drawdown - dd_threshold * 2) / 0.30
                ) * 0.3
            else:
                component_scores["tga_drawdown"] = 0.0

            evidence["tga_balance_millions"] = round(current_tga, 0)
            evidence["tga_recent_peak_millions"] = round(recent_peak, 0)
            evidence["tga_drawdown_pct"] = round(tga_drawdown * 100, 1)
            evidence["tga_below_stress_floor"] = current_tga < tga_stress_floor_millions
            source_series.append("WTREGEN")

        # GFDEBTN in millions, quarterly -- accelerating debt growth
        if total_debt is not None and not total_debt.empty and len(total_debt) >= 2:
            current_debt = float(total_debt.iloc[-1])
            prev_debt = float(total_debt.iloc[-2])

            if prev_debt > 0:
                debt_qoq_growth = (current_debt - prev_debt) / prev_debt
            else:
                debt_qoq_growth = 0.0

            # >2% quarterly growth = concerning (~8% annualized)
            if debt_qoq_growth > 0.02:
                component_scores["debt_momentum"] = min(
                    1.0, (debt_qoq_growth - 0.02) / 0.03
                )
            else:
                component_scores["debt_momentum"] = 0.0

            evidence["total_debt_millions"] = round(current_debt, 0)
            evidence["debt_qoq_growth_pct"] = round(debt_qoq_growth * 100, 2)
            source_series.append("GFDEBTN")

        # RRPONTSYD in billions -- near-zero = liquidity buffer exhausted
        if reverse_repo is not None and not reverse_repo.empty:
            current_rrp = float(reverse_repo.iloc[-1])

            # RRP was ~$2.5T in mid-2023; near zero means buffer is gone
            # Score inversely: lower RRP = higher stress
            if current_rrp < 100.0:
                component_scores["rrp_drainage"] = min(
                    1.0, (100.0 - current_rrp) / 100.0
                )
            else:
                component_scores["rrp_drainage"] = 0.0

            evidence["reverse_repo_billions"] = round(current_rrp, 2)
            source_series.append("RRPONTSYD")

        # WALCL in millions, weekly -- expansion during "tightening" = stealth QE
        if fed_assets is not None and not fed_assets.empty and len(fed_assets) >= 2:
            current_assets = float(fed_assets.iloc[-1])
            # 4-week lookback for momentum
            lookback = min(4, len(fed_assets))
            prev_assets = float(fed_assets.iloc[-lookback])

            if prev_assets > 0:
                fed_mom_change = (current_assets - prev_assets) / prev_assets
            else:
                fed_mom_change = 0.0

            # Positive growth during QT = stealth accommodation
            if fed_mom_change > 0.001:
                component_scores["fed_bs_momentum"] = min(1.0, fed_mom_change / 0.01)
            else:
                component_scores["fed_bs_momentum"] = 0.0

            evidence["fed_assets_billions"] = round(current_assets / 1000.0, 1)
            evidence["fed_assets_mom_change_pct"] = round(fed_mom_change * 100, 3)
            source_series.append("WALCL")

        if bank_reserves is not None and not bank_reserves.empty:
            evidence["bank_reserves_billions"] = round(
                float(bank_reserves.iloc[-1]) / 1000.0, 1
            )
            source_series.append("WRESBAL")

        if not component_scores:
            return signals

        weights = {
            "interest_receipts": 0.35,
            "tga_drawdown": 0.20,
            "debt_momentum": 0.15,
            "rrp_drainage": 0.15,
            "fed_bs_momentum": 0.15,
        }

        weighted_sum = sum(
            component_scores.get(k, 0.0) * w for k, w in weights.items()
        )
        active_weight = sum(w for k, w in weights.items() if k in component_scores)
        composite_severity = weighted_sum / active_weight if active_weight > 0 else 0.0

        base_confidence = thresholds.get("min_confidence", 0.75)
        confidence = min(0.95, base_confidence + len(component_scores) * 0.03)

        evidence["component_scores"] = {
            k: round(v, 3) for k, v in component_scores.items()
        }
        evidence["component_weights"] = {
            k: w for k, w in weights.items() if k in component_scores
        }
        evidence["composite_severity"] = round(composite_severity, 3)
        evidence["components_available"] = len(component_scores)
        evidence["components_total"] = len(weights)

        seen: set[str] = set()
        unique_sources = [s for s in source_series if not (s in seen or seen.add(s))]

        if composite_severity > 0.10:
            signals.append(
                BottleneckSignalData(
                    category=BottleneckCategory.FISCAL_DOMINANCE,
                    subcategory="composite_fiscal_stress",
                    severity=min(1.0, composite_severity),
                    confidence=confidence,
                    affected_sectors=["FINANCE", "GOVERNMENT", "CONSUMER"],
                    source_series=unique_sources,
                    evidence=evidence,
                    description=(
                        f"Fiscal dominance composite: {composite_severity:.0%} stress "
                        f"({len(component_scores)}/{len(weights)} indicators"
                        f"{', I/R=' + str(round(ir_ratio, 3)) if ir_ratio is not None else ''})"
                    ),
                )
            )

        return signals

    async def _detect_generic(
        self,
        category: BottleneckCategory,
        series_data: dict[str, pd.Series],
        anomalies: dict[str, list[DetectedAnomaly]],
        thresholds: dict[str, float],
    ) -> list[BottleneckSignalData]:
        """Generic anomaly-based bottleneck detection.

        Fallback for any category that does not have a dedicated detector.
        Emits one signal per series with significant anomalies.
        """
        signals: list[BottleneckSignalData] = []
        z_threshold = thresholds.get("z_score_threshold", 2.0)
        min_confidence = thresholds.get("min_confidence", 0.6)

        for series_id, series_anomalies in anomalies.items():
            significant = [a for a in series_anomalies if abs(a.z_score) >= z_threshold]

            if significant:
                max_severity = max(a.severity for a in significant)

                signals.append(
                    BottleneckSignalData(
                        category=category,
                        subcategory="anomaly_detected",
                        severity=max_severity,
                        confidence=min_confidence,
                        source_series=[series_id],
                        anomalies=[
                            AnomalyData(
                                series_id=series_id,
                                timestamp=a.timestamp,
                                actual_value=a.value,
                                expected_value=a.expected_value,
                                z_score=a.z_score,
                                detection_method=a.detection_method,
                                anomaly_type=a.anomaly_type,
                            )
                            for a in significant
                        ],
                        description=f"Anomaly detected in {series_id} related to {category.value}",
                    )
                )

        return signals
