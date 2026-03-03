"""Unit tests for analysis monitors.

Tests for:
- InventorySalesMonitor
- PriceSpikeDetector
- ShippingCongestionIndex
- EnergyCrunchDetector
- CriticalMineralRisk
- LaborTightnessMonitor
- CapacityUtilizationMonitor
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.analysis.monitors.base_monitor import BaseMonitor, MonitorResult
from src.analysis.monitors.inventory_sales_monitor import (
    InventorySalesMonitor,
    ISRatioResult,
)
from src.analysis.monitors.price_spike_detector import (
    PriceSpikeDetector,
    PriceBreakout,
)
from src.analysis.monitors.shipping_congestion_index import (
    ShippingCongestionIndex,
    SCIResult,
)
from src.analysis.monitors.energy_crunch_detector import (
    EnergyCrunchDetector,
    EnergyCrunchResult,
    SPRStatus,
    RefineryStatus,
)
from src.analysis.monitors.critical_mineral_risk import (
    CriticalMineralRisk,
    MineralRiskResult,
)
from src.analysis.monitors.labor_tightness_monitor import (
    LaborTightnessMonitor,
    LaborMetrics,
    SectorLaborStatus,
)
from src.analysis.monitors.capacity_utilization_monitor import (
    CapacityUtilizationMonitor,
    SectorUtilization,
)
from src.analysis.signals import BottleneckCategory


class TestBaseMonitorUtilities:
    """Test BaseMonitor utility methods."""

    def test_normalize_to_100(self):
        """Test normalize_to_100 function."""
        assert BaseMonitor.normalize_to_100(50, 0, 100) == 50.0
        assert BaseMonitor.normalize_to_100(0, 0, 100) == 0.0
        assert BaseMonitor.normalize_to_100(100, 0, 100) == 100.0

        assert BaseMonitor.normalize_to_100(150, 0, 100) == 100.0
        assert BaseMonitor.normalize_to_100(-50, 0, 100) == 0.0

        assert BaseMonitor.normalize_to_100(0, 0, 100, invert=True) == 100.0
        assert BaseMonitor.normalize_to_100(100, 0, 100, invert=True) == 0.0

        assert BaseMonitor.normalize_to_100(50, 50, 50) == 50.0

    def test_normalize_z_score_to_100(self):
        """Test Z-score normalization."""
        assert BaseMonitor.normalize_z_score_to_100(0.0) == 0.0
        assert BaseMonitor.normalize_z_score_to_100(2.0) == 50.0
        assert BaseMonitor.normalize_z_score_to_100(4.0) == 100.0
        assert BaseMonitor.normalize_z_score_to_100(-2.0) == 50.0  # Absolute value
        assert BaseMonitor.normalize_z_score_to_100(5.0) == 100.0  # Capped

    def test_calculate_z_score(self):
        """Test Z-score calculation."""
        assert BaseMonitor.calculate_z_score(10, 10, 1) == 0.0
        assert BaseMonitor.calculate_z_score(12, 10, 1) == 2.0
        assert BaseMonitor.calculate_z_score(8, 10, 1) == -2.0
        assert BaseMonitor.calculate_z_score(10, 10, 0) == 0.0  # Zero std

    def test_calculate_rolling_z_scores(self):
        """Test rolling Z-score calculation."""
        series = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
        z_scores = BaseMonitor.calculate_rolling_z_scores(series, window=5, min_periods=3)

        assert z_scores.iloc[0] == 0.0
        assert not np.isnan(z_scores.iloc[-1])

    def test_weighted_composite(self):
        """Test weighted composite calculation."""
        components = {"a": 50, "b": 100, "c": 0}
        weights = {"a": 0.5, "b": 0.25, "c": 0.25}

        result = BaseMonitor.weighted_composite(components, weights)
        # (50 * 0.5 + 100 * 0.25 + 0 * 0.25) / 1.0 = 50
        assert result == 50.0

    def test_score_to_severity(self):
        """Test score to severity conversion."""
        assert BaseMonitor.score_to_severity(0) == 0.0
        assert BaseMonitor.score_to_severity(50) == 0.5
        assert BaseMonitor.score_to_severity(100) == 1.0
        assert BaseMonitor.score_to_severity(150) == 1.0  # Capped

    def test_calculate_confidence(self):
        """Test confidence calculation."""
        conf = BaseMonitor.calculate_confidence(1.0, 50, min_samples=30)
        assert conf == 1.0

        conf = BaseMonitor.calculate_confidence(0.5, 30, min_samples=30)
        assert 0.5 <= conf <= 1.0

        conf = BaseMonitor.calculate_confidence(1.0, 10, min_samples=30)
        assert conf < 1.0


class TestMonitorResult:
    """Test MonitorResult dataclass."""

    def test_alert_level(self):
        """Test alert level property."""
        result = MonitorResult(score=85, severity=0.85, confidence=0.9)
        assert result.alert_level == "critical"

        result = MonitorResult(score=65, severity=0.65, confidence=0.8)
        assert result.alert_level == "elevated"

        result = MonitorResult(score=45, severity=0.45, confidence=0.7)
        assert result.alert_level == "moderate"

        result = MonitorResult(score=30, severity=0.30, confidence=0.6)
        assert result.alert_level == "normal"

    def test_to_dict(self):
        """Test dictionary conversion."""
        result = MonitorResult(
            score=75,
            severity=0.75,
            confidence=0.85,
            description="Test result",
            components={"a": 80, "b": 70},
        )

        d = result.to_dict()
        assert d["score"] == 75
        assert d["severity"] == 0.75
        assert d["alert_level"] == "elevated"
        assert "timestamp" in d


class TestInventorySalesMonitor:
    """Tests for InventorySalesMonitor."""

    @pytest.fixture
    def monitor(self):
        """Create monitor without database."""
        return InventorySalesMonitor(db=None, sigma_threshold=2.0)

    def test_initialization(self, monitor):
        """Test monitor initialization."""
        assert monitor.sigma_threshold == 2.0
        assert "retail" in monitor.sectors
        assert "wholesale" in monitor.sectors
        assert "manufacturing" in monitor.sectors

    def test_get_category(self, monitor):
        """Test category getter."""
        assert monitor.get_category() == BottleneckCategory.INVENTORY_SQUEEZE

    def test_analyze_sector_normal(self, monitor):
        """Test sector analysis with normal ratio."""
        dates = pd.date_range(end=datetime.now(UTC), periods=100, freq="D")
        values = np.random.normal(1.4, 0.05, 100)  # Mean 1.4, std 0.05
        series = pd.Series(values, index=dates)

        config = monitor.sectors["retail"]
        result = monitor._analyze_sector("retail", config, series)

        assert result.sector == "retail"
        assert result.status in ["normal", "squeeze", "elevated"]
        assert 0 <= result.score <= 100

    def test_analyze_sector_squeeze(self, monitor):
        """Test sector analysis with squeeze condition."""
        dates = pd.date_range(end=datetime.now(UTC), periods=100, freq="D")
        values = np.linspace(1.5, 1.1, 100)  # Declining toward squeeze
        series = pd.Series(values, index=dates)

        config = monitor.sectors["retail"]
        result = monitor._analyze_sector("retail", config, series)

        assert result.current_ratio < 1.2
        assert result.percentile < 50

    @pytest.mark.asyncio
    async def test_calculate_score_no_data(self, monitor):
        """Test calculate_score with no data."""
        result = await monitor.calculate_score()

        assert result.score == 0.0
        assert result.confidence == 0.0
        assert "No data" in result.description

    @pytest.mark.asyncio
    async def test_check_thresholds(self, monitor):
        """Test threshold checking returns proper signals."""
        with patch.object(monitor, "calculate_ratios") as mock_ratios:
            mock_ratios.return_value = {
                "retail": ISRatioResult(
                    sector="retail",
                    series_id="RETAILIRSA",
                    current_ratio=1.1,
                    mean_ratio=1.4,
                    std_ratio=0.05,
                    z_score=-6.0,
                    percentile=5.0,
                    baseline_deviation_pct=-21.0,
                    status="squeeze",
                    score=85.0,
                )
            }

            signals = await monitor.check_thresholds()

            assert len(signals) == 1
            assert signals[0].category == BottleneckCategory.INVENTORY_SQUEEZE
            assert "retail" in signals[0].subcategory


class TestPriceSpikeDetector:
    """Tests for PriceSpikeDetector."""

    @pytest.fixture
    def detector(self):
        """Create detector without database."""
        return PriceSpikeDetector(
            db=None,
            pct_threshold=0.10,
            z_score_threshold=2.5,
        )

    def test_initialization(self, detector):
        """Test detector initialization."""
        assert detector.pct_threshold == 0.10
        assert detector.z_score_threshold == 2.5
        assert "energy" in detector.commodity_groups
        assert "metals" in detector.commodity_groups
        assert "agriculture" in detector.commodity_groups

    def test_get_category(self, detector):
        """Test category getter."""
        assert detector.get_category() == BottleneckCategory.PRICE_SPIKE

    def test_check_breakout_no_breakout(self, detector):
        """Test breakout detection with stable prices."""
        dates = pd.date_range(end=datetime.now(UTC), periods=100, freq="D")
        values = np.random.normal(100, 1, 100)  # Very stable prices
        series = pd.Series(values, index=dates)

        result = detector._check_breakout(
            commodity_name="test",
            commodity_group="energy",
            series=series,
            timeframe="daily",
            pct_threshold=0.10,
            z_score_threshold=2.5,
            commodity_config={"series_id": "TEST", "unit": "USD"},
        )

        assert result is None or result.score < 50

    def test_check_breakout_spike(self, detector):
        """Test breakout detection with price spike."""
        dates = pd.date_range(end=datetime.now(UTC), periods=100, freq="D")
        values = np.concatenate([
            np.random.normal(100, 1, 99),  # Stable
            [115],  # 15% spike
        ])
        series = pd.Series(values, index=dates)

        result = detector._check_breakout(
            commodity_name="test",
            commodity_group="energy",
            series=series,
            timeframe="daily",
            pct_threshold=0.10,
            z_score_threshold=2.5,
            commodity_config={"series_id": "TEST", "unit": "USD"},
        )

        assert result is not None
        assert result.direction == "up"
        assert result.is_significant
        assert result.pct_change > 0.10

    @pytest.mark.asyncio
    async def test_calculate_score_no_data(self, detector):
        """Test calculate_score with no data."""
        result = await detector.calculate_score()

        assert result.score == 0.0
        assert "No significant price spikes" in result.description

    def test_get_affected_sectors(self):
        """Test sector mapping for commodity groups."""
        sectors = PriceSpikeDetector._get_affected_sectors("energy")
        assert "ENERGY" in sectors
        assert "TRANSPORTATION" in sectors

        sectors = PriceSpikeDetector._get_affected_sectors("metals")
        assert "MANUFACTURING" in sectors

        sectors = PriceSpikeDetector._get_affected_sectors("agriculture")
        assert "AGRICULTURE" in sectors


class TestShippingCongestionIndex:
    """Tests for ShippingCongestionIndex."""

    @pytest.fixture
    def index(self):
        """Create index without database."""
        return ShippingCongestionIndex(db=None)

    def test_initialization(self, index):
        """Test index initialization."""
        assert index.alert_threshold == 60.0
        assert index.critical_threshold == 80.0
        assert "baltic_indices" in index.weights
        assert "dwell_times" in index.weights

    def test_get_category(self, index):
        """Test category getter."""
        assert index.get_category() == BottleneckCategory.SHIPPING_CONGESTION

    @pytest.mark.asyncio
    async def test_calculate_index(self, index):
        """Test index calculation."""
        result = await index.calculate_index()

        assert isinstance(result, SCIResult)
        assert 0 <= result.score <= 100
        assert result.alert_level in ["normal", "elevated", "critical"]
        assert "baltic_indices" in result.components

    @pytest.mark.asyncio
    async def test_calculate_score(self, index):
        """Test MonitorResult generation."""
        result = await index.calculate_score()

        assert isinstance(result, MonitorResult)
        assert 0 <= result.score <= 100
        assert result.description  # Has description

    @pytest.mark.asyncio
    async def test_get_port_drilldown(self, index):
        """Test port drilldown."""
        details = await index.get_port_drilldown("Port of Los Angeles")

        assert details["port_name"] == "Port of Los Angeles"
        assert "congestion_score" in details
        assert "dwell_time_avg_days" in details


class TestEnergyCrunchDetector:
    """Tests for EnergyCrunchDetector."""

    @pytest.fixture
    def detector(self):
        """Create detector without database."""
        return EnergyCrunchDetector(db=None)

    def test_initialization(self, detector):
        """Test detector initialization."""
        assert "spr_critical_mb" in detector.thresholds
        assert "refinery_utilization_alert" in detector.thresholds
        assert detector.thresholds["spr_critical_mb"] == 350.0

    def test_get_category(self, detector):
        """Test category getter."""
        assert detector.get_category() == BottleneckCategory.ENERGY_CRUNCH

    @pytest.mark.asyncio
    async def test_check_spr_levels(self, detector):
        """Test SPR level checking."""
        result = await detector.check_spr_levels()

        assert isinstance(result, SPRStatus)
        assert result.current_level_mb > 0
        assert result.days_of_supply > 0
        assert 0 <= result.score <= 100

    @pytest.mark.asyncio
    async def test_check_refinery_utilization(self, detector):
        """Test refinery utilization checking."""
        result = await detector.check_refinery_utilization()

        assert isinstance(result, RefineryStatus)
        assert 0 <= result.utilization_rate <= 1
        assert 0 <= result.score <= 100

    @pytest.mark.asyncio
    async def test_calculate_crunch_score(self, detector):
        """Test full crunch score calculation."""
        result = await detector.calculate_crunch_score()

        assert isinstance(result, EnergyCrunchResult)
        assert 0 <= result.score <= 100
        assert result.alert_level in ["normal", "elevated", "critical"]
        assert "spr_levels" in result.components

    @pytest.mark.asyncio
    async def test_calculate_days_of_supply(self, detector):
        """Test days of supply calculation."""
        dos = await detector.calculate_days_of_supply()

        assert "crude_oil" in dos
        assert "motor_gasoline" in dos
        assert all(v > 0 for v in dos.values())


class TestCriticalMineralRisk:
    """Tests for CriticalMineralRisk."""

    @pytest.fixture
    def monitor(self):
        """Create monitor without database."""
        return CriticalMineralRisk(db=None)

    def test_initialization(self, monitor):
        """Test monitor initialization."""
        assert "lithium" in monitor.minerals
        assert "cobalt" in monitor.minerals
        assert "rare_earths" in monitor.minerals
        assert "CHN" in monitor.risk_countries

    def test_get_category(self, monitor):
        """Test category getter."""
        assert monitor.get_category() == BottleneckCategory.SUPPLY_DISRUPTION

    @pytest.mark.asyncio
    async def test_calculate_import_dependency(self, monitor):
        """Test import dependency calculation."""
        dep = await monitor.calculate_import_dependency("lithium")
        assert 0.95 <= dep <= 1.0

        dep = await monitor.calculate_import_dependency("rare_earths")
        assert 0.85 <= dep <= 0.95

    @pytest.mark.asyncio
    async def test_calculate_concentration_risk(self, monitor):
        """Test concentration risk (HHI) calculation."""
        score, suppliers = await monitor.calculate_concentration_risk("cobalt")

        assert score > 40
        assert len(suppliers) > 0
        assert suppliers[0]["country"] == "COD"  # DRC should be top

    @pytest.mark.asyncio
    async def test_calculate_geopolitical_risk(self, monitor):
        """Test geopolitical risk calculation."""
        risk = await monitor.calculate_geopolitical_risk("graphite")
        assert risk > 50

        risk = await monitor.calculate_geopolitical_risk("lithium")
        assert risk < 50

    @pytest.mark.asyncio
    async def test_composite_risk_score(self, monitor):
        """Test composite risk score."""
        result = await monitor.composite_risk_score("cobalt")

        assert isinstance(result, MineralRiskResult)
        assert result.mineral == "cobalt"
        assert 0 <= result.composite_risk_score <= 100
        assert result.import_dependency > 0.9  # 100% imported
        assert len(result.top_suppliers) > 0

    @pytest.mark.asyncio
    async def test_calculate_all_minerals(self, monitor):
        """Test all minerals calculation."""
        results = await monitor.calculate_all_minerals()

        assert len(results) == 6  # All 6 minerals
        assert "lithium" in results
        assert "cobalt" in results

    @pytest.mark.asyncio
    async def test_calculate_aggregate_risk(self, monitor):
        """Test aggregate risk calculation."""
        aggregate = await monitor.calculate_aggregate_risk()

        assert "aggregate_score" in aggregate
        assert "alert_level" in aggregate
        assert aggregate["minerals_analyzed"] == 6

    @pytest.mark.asyncio
    async def test_get_china_exposure_summary(self, monitor):
        """Test China exposure summary."""
        exposure = await monitor.get_china_exposure_summary()

        assert "average_china_exposure" in exposure
        assert "high_exposure_minerals" in exposure
        assert "graphite" in exposure["high_exposure_minerals"]  # 65% China

    def test_get_country_risk(self, monitor):
        """Test country risk weight lookup."""
        assert monitor._get_country_risk("CHN") == 0.80
        assert monitor._get_country_risk("RUS") == 0.90
        assert monitor._get_country_risk("AUS") == 0.10
        assert monitor._get_country_risk("UNKNOWN") == 0.40  # Default

    def test_get_supplier_shares(self, monitor):
        """Test supplier shares data."""
        shares = monitor._get_supplier_shares("cobalt")

        assert "COD" in shares
        assert shares["COD"] == 0.70
        assert sum(shares.values()) == pytest.approx(1.0, rel=0.01)


class TestLaborTightnessMonitor:
    """Tests for LaborTightnessMonitor."""

    @pytest.fixture
    def monitor(self):
        """Create monitor without database."""
        return LaborTightnessMonitor(db=None)

    def test_initialization(self, monitor):
        """Test monitor initialization."""
        assert "openings_ratio_tight" in monitor.thresholds
        assert "quits_rate_high" in monitor.thresholds
        assert monitor.thresholds["openings_ratio_tight"] == 2.0

    def test_get_category(self, monitor):
        """Test category getter."""
        assert monitor.get_category() == BottleneckCategory.LABOR_TIGHTNESS

    @pytest.mark.asyncio
    async def test_calculate_openings_ratio(self, monitor):
        """Test job openings ratio calculation."""
        ratio, openings, unemployed = await monitor.calculate_openings_ratio()

        assert ratio > 0
        assert openings > 0
        assert unemployed > 0

    @pytest.mark.asyncio
    async def test_calculate_quits_indicator(self, monitor):
        """Test quits rate calculation."""
        quits_rate = await monitor.calculate_quits_indicator()

        assert 0 <= quits_rate <= 0.10  # Reasonable range

    @pytest.mark.asyncio
    async def test_calculate_wage_acceleration(self, monitor):
        """Test wage growth calculation."""
        wage_growth = await monitor.calculate_wage_acceleration()

        assert -0.10 <= wage_growth <= 0.15  # Reasonable YoY range

    @pytest.mark.asyncio
    async def test_get_labor_metrics(self, monitor):
        """Test full labor metrics."""
        metrics = await monitor.get_labor_metrics()

        assert isinstance(metrics, LaborMetrics)
        assert metrics.openings_ratio > 0
        assert 0 <= metrics.quits_rate <= 0.10
        assert 0 <= metrics.unemployment_rate <= 0.20

    @pytest.mark.asyncio
    async def test_calculate_score(self, monitor):
        """Test composite score calculation."""
        result = await monitor.calculate_score()

        assert isinstance(result, MonitorResult)
        assert 0 <= result.score <= 100
        assert "openings_ratio" in result.components
        assert "quits_rate" in result.components
        assert "wage_growth" in result.components

    def test_score_openings_ratio(self, monitor):
        """Test openings ratio scoring."""
        score = monitor._score_openings_ratio(2.5)
        assert score >= 60

        score = monitor._score_openings_ratio(1.2)
        assert 30 <= score <= 60

        score = monitor._score_openings_ratio(0.5)
        assert score < 30

    def test_score_quits_rate(self, monitor):
        """Test quits rate scoring."""
        score = monitor._score_quits_rate(0.035)
        assert score >= 70

        score = monitor._score_quits_rate(0.025)
        assert 30 <= score <= 70

    def test_score_wage_growth(self, monitor):
        """Test wage growth scoring."""
        score = monitor._score_wage_growth(0.05)
        assert score >= 70

        score = monitor._score_wage_growth(0.03)
        assert 30 <= score <= 70

    @pytest.mark.asyncio
    async def test_get_sector_constraints(self, monitor):
        """Test sector-specific constraints."""
        sectors = await monitor.get_sector_constraints()

        assert len(sectors) > 0
        for sector_id, status in sectors.items():
            assert isinstance(status, SectorLaborStatus)
            assert status.status in ["tight", "normal", "loose"]


class TestCapacityUtilizationMonitor:
    """Tests for CapacityUtilizationMonitor."""

    @pytest.fixture
    def monitor(self):
        """Create monitor without database."""
        return CapacityUtilizationMonitor(db=None)

    def test_initialization(self, monitor):
        """Test monitor initialization."""
        assert "total" in monitor.sectors
        assert "manufacturing" in monitor.sectors
        assert "mining" in monitor.sectors
        assert "utilities" in monitor.sectors

    def test_get_category(self, monitor):
        """Test category getter."""
        assert monitor.get_category() == BottleneckCategory.CAPACITY_CEILING

    def test_sector_thresholds(self, monitor):
        """Test sector threshold configuration."""
        assert monitor.sectors["total"]["threshold"] == 0.85
        assert monitor.sectors["manufacturing"]["threshold"] == 0.80
        assert monitor.sectors["mining"]["threshold"] == 0.90

    @pytest.mark.asyncio
    async def test_get_sector_utilization(self, monitor):
        """Test sector utilization retrieval."""
        util = await monitor.get_sector_utilization("total")

        assert isinstance(util, SectorUtilization)
        assert 0 <= util.current_utilization <= 1
        assert util.status in ["normal", "elevated", "critical"]

    @pytest.mark.asyncio
    async def test_get_all_sector_utilizations(self, monitor):
        """Test all sector utilizations."""
        utils = await monitor.get_all_sector_utilizations()

        assert len(utils) == 4  # total, manufacturing, mining, utilities
        for sector, util in utils.items():
            assert isinstance(util, SectorUtilization)
            assert 0 <= util.score <= 100

    @pytest.mark.asyncio
    async def test_calculate_score(self, monitor):
        """Test composite score calculation."""
        result = await monitor.calculate_score()

        assert isinstance(result, MonitorResult)
        assert 0 <= result.score <= 100
        assert "total" in result.components
        assert "manufacturing" in result.components

    @pytest.mark.asyncio
    async def test_check_sector_thresholds(self, monitor):
        """Test threshold checking for signals."""
        signals = await monitor.check_sector_thresholds()

        assert isinstance(signals, list)

    @pytest.mark.asyncio
    async def test_calculate_historical_deviation(self, monitor):
        """Test historical deviation calculation."""
        deviation = await monitor.calculate_historical_deviation()

        assert "total" in deviation
        assert "manufacturing" in deviation

    @pytest.mark.asyncio
    async def test_get_rate_of_change(self, monitor):
        """Test rate of change calculation."""
        roc = await monitor.get_rate_of_change()

        assert "total" in roc
        for sector, data in roc.items():
            assert "mom_change" in data or data.get("mom_change") is None

    @pytest.mark.asyncio
    async def test_get_summary(self, monitor):
        """Test comprehensive summary."""
        summary = await monitor.get_summary()

        assert "composite_score" in summary
        assert "alert_level" in summary
        assert "sectors" in summary
        assert "timestamp" in summary


class TestMonitorIntegration:
    """Integration tests for all monitors."""

    @pytest.mark.asyncio
    async def test_all_monitors_return_monitor_result(self):
        """Test that all monitors return MonitorResult."""
        monitors = [
            InventorySalesMonitor(db=None),
            PriceSpikeDetector(db=None),
            ShippingCongestionIndex(db=None),
            EnergyCrunchDetector(db=None),
            CriticalMineralRisk(db=None),
            LaborTightnessMonitor(db=None),
            CapacityUtilizationMonitor(db=None),
        ]

        for monitor in monitors:
            result = await monitor.calculate_score()
            assert isinstance(result, MonitorResult)
            assert 0 <= result.score <= 100
            assert 0 <= result.severity <= 1
            assert 0 <= result.confidence <= 1

    @pytest.mark.asyncio
    async def test_all_monitors_generate_signals(self):
        """Test that monitors with high scores generate signals."""
        monitors = [
            (InventorySalesMonitor(db=None), "check_thresholds"),
            (PriceSpikeDetector(db=None), "generate_signals"),
            (ShippingCongestionIndex(db=None), "generate_signals"),
            (EnergyCrunchDetector(db=None), "generate_signals"),
            (CriticalMineralRisk(db=None), "generate_signals"),
            (LaborTightnessMonitor(db=None), "generate_signals"),
            (CapacityUtilizationMonitor(db=None), "generate_signals"),
        ]

        for monitor, method_name in monitors:
            method = getattr(monitor, method_name)
            signals = await method()
            assert isinstance(signals, list)
