"""Signal definitions for bottleneck detection."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


class BottleneckCategory(str, Enum):
    """Categories of economic bottlenecks."""

    INVENTORY_SQUEEZE = "inventory_squeeze"
    PRICE_SPIKE = "price_spike"
    SHIPPING_CONGESTION = "shipping_congestion"
    LABOR_TIGHTNESS = "labor_tightness"
    CAPACITY_CEILING = "capacity_ceiling"
    DEMAND_SURGE = "demand_surge"
    SUPPLY_DISRUPTION = "supply_disruption"
    ENERGY_CRUNCH = "energy_crunch"
    CREDIT_TIGHTENING = "credit_tightening"
    SENTIMENT_SHIFT = "sentiment_shift"
    FISCAL_DOMINANCE = "fiscal_dominance"


class SignalStrength(str, Enum):
    """Strength of a bottleneck signal."""

    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    CRITICAL = "critical"


@dataclass
class AnomalyData:
    """Data about a detected anomaly."""

    series_id: str
    timestamp: datetime
    actual_value: float
    expected_value: float
    z_score: float
    detection_method: str
    anomaly_type: str  # "spike", "drop", "trend_break", etc.
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BottleneckSignalData:
    """A detected bottleneck signal."""

    id: UUID = field(default_factory=uuid4)
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    category: BottleneckCategory = BottleneckCategory.SUPPLY_DISRUPTION
    subcategory: str | None = None
    severity: float = 0.5  # 0-1 scale
    confidence: float = 0.5  # 0-1 scale
    affected_sectors: list[str] = field(default_factory=list)
    affected_commodities: list[str] = field(default_factory=list)
    source_series: list[str] = field(default_factory=list)
    anomalies: list[AnomalyData] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    description: str = ""
    status: str = "active"

    @property
    def strength(self) -> SignalStrength:
        """Calculate signal strength from severity and confidence."""
        combined = (self.severity + self.confidence) / 2
        if combined >= 0.8:
            return SignalStrength.CRITICAL
        elif combined >= 0.6:
            return SignalStrength.STRONG
        elif combined >= 0.4:
            return SignalStrength.MODERATE
        else:
            return SignalStrength.WEAK

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage/API."""
        return {
            "id": str(self.id),
            "detected_at": self.detected_at.isoformat(),
            "category": self.category.value,
            "subcategory": self.subcategory,
            "severity": self.severity,
            "confidence": self.confidence,
            "strength": self.strength.value,
            "affected_sectors": self.affected_sectors,
            "affected_commodities": self.affected_commodities,
            "source_series": self.source_series,
            "evidence": self.evidence,
            "description": self.description,
            "status": self.status,
        }


@dataclass
class SectorImpact:
    """Impact of a bottleneck on a sector."""

    sector_code: str
    sector_name: str
    impact_score: float  # 0-1 scale
    impact_type: str  # "direct", "indirect", "downstream"
    propagation_path: list[str] = field(default_factory=list)
    lag_days: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


# Detection thresholds by category
DETECTION_THRESHOLDS: dict[BottleneckCategory, dict[str, float]] = {
    BottleneckCategory.INVENTORY_SQUEEZE: {
        "z_score_threshold": 2.0,
        "ratio_threshold": 0.15,  # 15% below normal inventory/sales
        "min_confidence": 0.6,
    },
    BottleneckCategory.PRICE_SPIKE: {
        "z_score_threshold": 2.5,
        "pct_change_threshold": 0.10,  # 10% price increase
        "min_confidence": 0.7,
    },
    BottleneckCategory.SHIPPING_CONGESTION: {
        "z_score_threshold": 2.0,
        "dwell_time_threshold": 1.5,  # 50% above normal
        "min_confidence": 0.65,
    },
    BottleneckCategory.LABOR_TIGHTNESS: {
        "z_score_threshold": 2.0,
        "job_openings_ratio": 1.2,  # BLS JTSJOL/UNEMPLOY ratio; 1.2 = tight market
        "quits_rate_threshold": 2.5,  # FRED JTSQUR: 2.5% quits rate (Great Resignation peaked ~3.0)
        "lmci_activity_threshold": 0.5,  # KC Fed LMCI: 0.5 std above long-run avg = tight
        "hires_sep_ratio_threshold": 1.05,  # JTSHIR/JTSTSR > 1.05 = net tightening
        "min_confidence": 0.7,
    },
    BottleneckCategory.CAPACITY_CEILING: {
        "tcu_elevated": 80.0,
        "tcu_critical": 85.0,
        "z_score_threshold": 1.5,
        "min_confidence": 0.75,
    },
    BottleneckCategory.DEMAND_SURGE: {
        "single_series_surge_pct": 0.05,  # 5% YoY = surge for one series
        "dual_series_surge_pct": 0.03,  # 3% YoY = surge if both confirm
        "credit_growth_high": 0.08,  # 8% YoY consumer credit growth = credit-fueled
        "sentiment_divergence_threshold": -0.10,  # 10% below 12m avg = divergent
        "z_score_threshold": 2.5,
        "min_confidence": 0.65,
    },
    BottleneckCategory.SUPPLY_DISRUPTION: {
        "port_congestion_high_days": 5.0,
        "port_congestion_critical_days": 10.0,
        "shipping_cost_spike_pct": 0.30,
        "delivery_time_threshold": 10.0,
        "sec_signal_density_threshold": 0.15,
        "min_confidence": 0.65,
    },
    BottleneckCategory.ENERGY_CRUNCH: {
        "price_z_threshold": 2.0,
        "stocks_seasonal_threshold": 0.80,
        "refinery_util_high": 0.92,
        "refinery_util_low": 0.80,
        "price_momentum_threshold": 0.15,
        "min_confidence": 0.70,
    },
    BottleneckCategory.CREDIT_TIGHTENING: {
        "z_score_threshold": 2.0,
        "spread_threshold": 0.50,  # 50bps widening
        "min_confidence": 0.65,
    },
    BottleneckCategory.SENTIMENT_SHIFT: {
        "z_score_threshold": 2.0,
        "sentiment_drop": 0.10,
        "credit_card_delinq_threshold": 3.5,
        "consumer_delinq_threshold": 3.0,
        "student_delinq_threshold": 5.0,
        "savings_rate_critical": 3.0,
        "nfci_tight_threshold": 0.0,
        "nfib_pessimistic": 90,
        "nfib_recessionary": 85,
        "trends_stress_z_threshold": 1.5,
        "min_confidence": 0.55,
    },
    BottleneckCategory.FISCAL_DOMINANCE: {
        "interest_receipts_ratio_threshold": 0.25,  # 25% = Gromen's danger zone
        "tga_drawdown_threshold": -0.20,  # 20% drawdown from recent peak
        "z_score_threshold": 2.0,
        "min_confidence": 0.75,
    },
}

# Series mappings for each bottleneck category
CATEGORY_SERIES_MAP: dict[BottleneckCategory, list[str]] = {
    BottleneckCategory.INVENTORY_SQUEEZE: [
        "BUSINV",  # Business Inventories
        "AMTMNO",  # Manufacturers Inventories
        "RETAILIMSA",  # Retail Inventories
        "ISRATIO",  # Total Business Inventory-to-Sales Ratio
        "RETAILIRSA",  # Retail Inventory-to-Sales Ratio
        "MNFCTRIRSA",  # Manufacturers Inventory-to-Sales Ratio
        "DGORDER",  # Durable Goods Orders (for orders vs inventory divergence)
        "NEWORDER",  # Manufacturers New Orders
    ],
    BottleneckCategory.PRICE_SPIKE: [
        "CPIAUCSL",  # CPI
        "PPIACO",  # PPI
        "DCOILWTICO",  # WTI Oil
        "DHHNGSP",  # Natural Gas
        "GASREGW",  # Regular Gas Price
    ],
    BottleneckCategory.SHIPPING_CONGESTION: [
        "TSIFRGHT",  # Transportation Services Index
        "RAILFRTCARLOADSD11",  # Rail Freight
    ],
    BottleneckCategory.LABOR_TIGHTNESS: [
        # --- Structural tightness (Beveridge Curve / V-U ratio) ---
        "JTSJOL",  # Job Openings (thousands, SA) -- numerator for V/U ratio
        "UNEMPLOY",  # Unemployment Level (thousands, SA) -- denominator for V/U ratio
        "UNRATE",  # Unemployment Rate (%, SA) -- context / evidence
        # --- Flow dynamics (hiring vs leaving) ---
        "JTSHIR",  # JOLTS Hires Rate (%, SA) -- inflow
        "JTSTSR",  # JOLTS Total Separations Rate (%, SA) -- outflow
        "JTSQUR",  # JOLTS Quits Rate (%, SA) -- voluntary outflow (worker confidence)
        # --- Composite labor conditions (KC Fed) ---
        "FRBKCLMCILA",  # KC Fed LMCI Level of Activity (0 = long-run avg)
        "FRBKCLMCIM",  # KC Fed LMCI Momentum (change in activity)
        # --- Private sector employment momentum (ADP) ---
        "ADPWNUSNERSA",  # ADP Total Nonfarm Private Employment (persons, SA)
        # --- Context ---
        "PAYEMS",  # Total Nonfarm Payrolls (thousands, SA)
    ],
    BottleneckCategory.CAPACITY_CEILING: [
        "TCU",  # Capacity Utilization (total)
        "MCUMFN",  # Manufacturing Capacity Utilization
        "INDPRO",  # Industrial Production Index
    ],
    BottleneckCategory.DEMAND_SURGE: [
        "RSXFS",  # Retail Sales excl Food Services
        "PCE",  # Personal Consumption Expenditures
        "UMCSENT",  # Consumer Sentiment (qualifier, not fused)
        "TOTALSL",  # Consumer Credit (qualifier, not fused)
    ],
    BottleneckCategory.ENERGY_CRUNCH: [
        "DCOILWTICO",  # WTI Oil
        "DHHNGSP",  # Natural Gas
        "GASREGW",  # Regular Gas Price
        "WCSSTUS1",  # Crude Oil Commercial Stocks excl SPR
        "WGTSTUS1",  # Gasoline Stocks
        "WPULEUS3",  # Refinery Utilization Rate
    ],
    BottleneckCategory.CREDIT_TIGHTENING: [
        "DFF",  # Fed Funds Rate
        "DGS10",  # 10Y Treasury
        "T10Y2Y",  # Yield Curve
        "BAMLH0A0HYM2",  # High Yield Spread
    ],
    BottleneckCategory.SENTIMENT_SHIFT: [
        "UMCSENT",  # Consumer Sentiment (UMich)
        "CSCICP03USM665S",  # Consumer Confidence (OECD)
        "NFCI",  # Chicago Fed National Financial Conditions Index
        "DRCCLACBS",  # Delinquency Rate on Credit Card Loans
        "DRCLACBS",  # Delinquency Rate on Consumer Loans
        "DRSFRMACBS",  # Delinquency Rate on Student Loans
        "CCLACBW027SBOG",  # Credit Card Loans Outstanding
        "SLOAS",  # Student Loans Outstanding
        "PSAVERT",  # Personal Savings Rate
        "TOTALSL",  # Consumer Credit (for credit expansion rate)
    ],
    BottleneckCategory.SUPPLY_DISRUPTION: [
        "PORT_CONGESTION_US_COMPOSITE",  # Weighted US port congestion composite
        "PORT_CONGESTION_USLAX",  # Los Angeles port congestion
        "PORT_CONGESTION_USLGB",  # Long Beach port congestion
        "SHIPPING_POLA_TEU_TOTAL",  # Port of LA TEU volumes
        "SEC_10-K_RISK_FACTOR_KEYWORDS",  # SEC filing supply chain keyword density
        "SEC_10-Q_RISK_FACTOR_KEYWORDS",  # SEC filing supply chain keyword density
        "DTCDFNA066MNFRBPHI",  # Philly Fed Delivery Time Diffusion Index
    ],
    BottleneckCategory.FISCAL_DOMINANCE: [
        "A091RC1Q027SBEA",  # Federal Interest Payments (quarterly, SAAR, billions)
        "W006RC1Q027SBEA",  # Federal Tax Receipts (quarterly, SAAR, billions)
        "GFDEBTN",  # Total Public Debt Outstanding (quarterly, millions)
        "WTREGEN",  # Treasury General Account balance (weekly, millions)
        "RRPONTSYD",  # Overnight Reverse Repo (daily, billions)
        "WALCL",  # Fed Total Assets / Balance Sheet (weekly, millions)
        "WRESBAL",  # Reserve Balances at Fed (weekly, millions)
    ],
}
