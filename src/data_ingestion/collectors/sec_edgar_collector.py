"""SEC EDGAR filings collector for supply chain signal extraction.

Collects SEC filings (10-K, 10-Q, 8-K) and extracts supply chain related
signals by searching for keywords in risk factor sections and management
discussions.

API Documentation: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
Rate Limit: 10 requests/second (must declare User-Agent)
"""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()


@dataclass
class SECFiling:
    """Represents an SEC filing with extracted data."""

    cik: str
    company_name: str
    ticker: str | None
    filing_type: str
    filing_date: datetime
    accession_number: str
    primary_document: str
    file_url: str
    sic_code: str | None = None
    sic_description: str | None = None


@dataclass
class FilingSignal:
    """Signal extracted from a filing."""

    filing: SECFiling
    signal_type: str  # keyword_match, risk_factor, etc.
    extracted_text: str
    keyword_matches: dict[str, int] = field(default_factory=dict)
    sentiment_score: float | None = None
    section: str | None = None  # risk_factors, mda, etc.


class SECEdgarCollector(BaseCollector):
    """Collector for SEC EDGAR filings with supply chain signal extraction.

    Features:
    - Fetches recent filings by form type (10-K, 10-Q, 8-K)
    - Full-text search for supply chain keywords
    - Extracts risk factor sections
    - Tracks companies by sector for bottleneck correlation
    """

    # API endpoints
    BASE_URL = "https://data.sec.gov"
    EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
    FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index"

    # Filing types to monitor
    FILING_TYPES = ["10-K", "10-Q", "8-K"]

    # Supply chain keywords for text mining
    # Includes both direct terms AND corporate euphemisms/soft language
    # Companies use euphemistic language to obscure negative realities in filings
    SUPPLY_CHAIN_KEYWORDS = [
        "supply chain",
        "supply network",
        "value chain",
        "supply base",
        "sourcing",
        "supply chain challenges",  # Euphemism: problems
        "supply chain headwinds",  # Euphemism: problems
        "supply chain dynamics",  # Euphemism: instability
        "supply chain optimization",  # Euphemism: fixing problems
        "supply chain resilience",  # Euphemism: past/current issues
        "supply chain visibility",  # Euphemism: lack of control
        "supply chain complexity",  # Euphemism: hard to manage
        "supply chain evolution",  # Euphemism: forced changes
        "supply chain agility",  # Euphemism: struggling to adapt
        "end-to-end supply chain",  # Often when discussing issues
        "shortage",
        "shortages",
        "scarcity",
        "deficit",
        "undersupply",
        "shortfall",
        "in short supply",
        "limited availability",
        "allocation",  # Euphemism: rationing due to shortage
        "rationing",
        "constrained supply",  # Euphemism: shortage
        "supply tightness",  # Euphemism: shortage
        "supply imbalance",  # Euphemism: shortage
        "demand-supply mismatch",  # Euphemism: shortage
        "availability challenges",  # Euphemism: shortage
        "allocation environment",  # Euphemism: rationing
        "supply situation",  # Euphemism: problems
        "supply dynamics",  # Euphemism: shortage/instability
        "product availability",  # When discussing limitations
        "limited quantities",  # Euphemism: shortage
        "supply recovery",  # Euphemism: was/is a shortage
        # =======================================================================
        # BACKLOG/ORDERS - Direct and Euphemistic
        # =======================================================================
        "backlog",
        "backlogs",
        "order backlog",
        "unfilled orders",
        "pending orders",
        "delivery backlog",
        "order book",  # When "elevated" = backlog
        "order fulfillment",  # When discussing issues
        "elevated orders",  # Euphemism: backlog
        "strong order book",  # Sometimes masks delivery issues
        "order timing",  # Euphemism: delays/cancellations
        "order patterns",  # Euphemism: irregular demand
        "inventory",
        "inventories",
        "stock levels",
        "stockpile",
        "safety stock",
        "buffer stock",
        "working capital",  # Often tied to inventory issues
        "inventory build",  # Euphemism: supply concerns
        "inventory drawdown",  # Euphemism: shortage
        "days of supply",
        "inventory management",  # When discussing challenges
        "inventory optimization",  # Euphemism: problems
        "inventory position",  # When discussing concerns
        "channel inventory",  # When discussing imbalances
        "inventory destocking",  # Euphemism: customers not buying
        "inventory normalization",  # Euphemism: excess being cleared
        "inventory levels",  # When discussing concerns
        "inventory rebalancing",  # Euphemism: had wrong mix
        "raw materials",
        "feedstock",
        "inputs",
        "components",
        "parts",
        "materials",
        "subcomponents",
        "bill of materials",
        "critical materials",
        "key components",
        "input availability",  # Euphemism: shortage
        "materials sourcing",  # When discussing challenges
        "critical inputs",  # Risk indicator
        "strategic materials",  # Risk indicator
        "key inputs",  # When discussing availability
        "supplier",
        "suppliers",
        "vendor",
        "vendors",
        "vendor base",
        "sourcing partner",
        "contract manufacturer",
        "tier-one supplier",
        "tier-two supplier",
        "supplier concentration",  # Risk indicator
        "supplier diversification",  # Euphemism: past issues
        "supplier performance",  # Euphemism: problems
        "supplier reliability",  # Euphemism: concerns
        "supplier capacity",  # When constrained
        "single source",
        "sole source",
        "dual sourcing",  # Euphemism: risk mitigation
        "multi-sourcing",  # Euphemism: risk mitigation
        "supplier qualification",  # Euphemism: switching issues
        "supplier relationships",  # When discussing challenges
        "vendor management",  # When discussing issues
        "supplier base optimization",  # Euphemism: changes needed
        "strategic suppliers",  # Risk indicator for concentration
        "logistics",
        "distribution",
        "fulfillment",
        "warehousing",
        "storage",
        "cargo",
        "transit",
        "freight",
        "transportation",
        "shipping",
        "delivery",
        "last mile",
        "logistics challenges",  # Euphemism: problems
        "fulfillment challenges",  # Euphemism: problems
        "distribution constraints",  # Euphemism: problems
        "logistics capacity",  # When limited
        "transportation constraints",  # Euphemism: problems
        "logistics network",  # When discussing optimization
        "distribution network",  # When discussing issues
        "fulfillment capabilities",  # When discussing limitations
        "logistics costs",  # When increasing
        "freight costs",  # When increasing
        "transportation costs",  # When increasing
        "lead time",
        "lead times",
        "delivery time",
        "turnaround time",
        "extended lead time",
        "order-to-delivery",
        "delayed shipments",
        "late deliveries",
        "shipping delays",
        "transit delays",
        "extended timelines",  # Euphemism: delays
        "timing shifts",  # Euphemism: delays
        "schedule adjustments",  # Euphemism: delays
        "delivery challenges",  # Euphemism: delays
        "timing uncertainty",  # Euphemism: delays
        "schedule volatility",  # Euphemism: delays
        "longer cycle times",  # Euphemism: delays
        "project delays",  # Direct
        "launch timing",  # Euphemism: delayed launch
        "timing impacts",  # Euphemism: delays
        "timeline extensions",  # Euphemism: delays
        "revised timeline",  # Euphemism: delays
        "updated schedule",  # Euphemism: delays
        "shifted timing",  # Euphemism: delays
        "manufacturing capacity",
        "production capacity",
        "output capacity",
        "plant capacity",
        "utilization",
        "capacity expansion",
        "capacity reduction",
        "capacity constraints",
        "capacity alignment",  # Euphemism: mismatch
        "capacity rationalization",  # Euphemism: cuts
        "optimization initiatives",  # Euphemism: cuts
        "rightsizing",  # Euphemism: cuts
        "capacity investments",  # Euphemism: prior shortage
        "capacity additions",  # Euphemism: prior shortage
        "operating at capacity",  # Indicates constraints
        "capacity utilization",  # When discussing constraints
        "production optimization",  # Euphemism: capacity issues
        "operational capacity",  # When discussing limits
        "capacity flexibility",  # When lacking = problem
        "disruption",
        "disruptions",
        "interruption",
        "setback",
        "impediment",
        "outage",
        "force majeure",
        "natural disaster",
        "weather event",
        "bottleneck",
        "bottlenecks",
        "constraint",
        "chokepoint",
        "operational challenges",  # Euphemism: problems
        "business continuity",  # Indicates disruption planning
        "contingency planning",  # Indicates risks
        "risk mitigation",  # Indicates existing risks
        "supply disruption",
        "production disruption",
        "operational disruption",
        "service disruption",
        "business interruption",
        "recovery efforts",  # Euphemism: dealing with disruption
        "restoration",  # Euphemism: recovering from problem
        "remediation",  # Euphemism: fixing problems
        "cost inflation",
        "cost increase",
        "price increase",
        "margin pressure",
        "input costs",
        "commodity prices",
        "cost pressures",
        "pricing actions",  # Euphemism: raising prices
        "pricing initiatives",  # Euphemism: raising prices
        "cost recovery",  # Euphemism: raising prices
        "price adjustments",  # Euphemism: raising prices
        "inflationary pressures",  # Euphemism: cost increases
        "cost headwinds",  # Euphemism: cost increases
        "margin compression",  # Result of cost increases
        "cost environment",  # Euphemism: high costs
        "input cost dynamics",  # Euphemism: cost volatility
        "cost mitigation",  # Indicates cost problems
        "cost reduction initiatives",  # Indicates margin pressure
        "pricing power",  # When lacking = margin pressure
        "pass-through",  # Passing costs to customers
        "surcharges",  # Additional fees due to costs
        "cost absorption",  # Eating cost increases
        "value realization",  # Euphemism: raising prices
        "value capture",  # Euphemism: raising prices
        "commercial excellence",  # Euphemism: raising prices
        "pricing optimization",  # Euphemism: raising prices
        "strategic pricing",  # Euphemism: raising prices
        "mix improvement",  # Euphemism: selling pricier items
        "favorable mix",  # Euphemism: price increases via mix
        "revenue management",  # Euphemism: raising prices
        "price realization",  # Euphemism: successful price hikes
        "net revenue management",  # Euphemism: price optimization
        "trade spend optimization",  # Euphemism: reducing discounts
        "promotional efficiency",  # Euphemism: fewer sales
        "reduced promotional activity",  # Raising effective prices
        "list price increases",  # Direct
        "pricing elasticity",  # When discussing ability to raise
        "price-cost gap",  # Margin pressure indicator
        "cost savings",  # Often indicates prior cost problems
        "productivity improvements",  # Euphemism: cost cutting
        "efficiency gains",  # Euphemism: cost cutting
        "restructuring",  # Euphemism: layoffs
        "reorganization",  # Euphemism: layoffs
        "organizational redesign",  # Euphemism: layoffs
        "workforce optimization",  # Euphemism: layoffs
        "headcount reduction",  # Euphemism: layoffs
        "streamlining",  # Euphemism: layoffs/cuts
        "efficiency measures",  # Euphemism: layoffs
        "cost reduction program",  # Often includes layoffs
        "organizational efficiency",  # Euphemism: layoffs
        "right-sizing",  # Euphemism: layoffs
        "resource rebalancing",  # Euphemism: layoffs
        "organizational simplification",  # Euphemism: layoffs
        "span of control",  # Euphemism: management cuts
        "voluntary separation",  # Euphemism: buyouts
        "involuntary separation",  # Euphemism: layoffs
        "reduction in force",  # Euphemism: layoffs
        "position elimination",  # Euphemism: layoffs
        "role consolidation",  # Euphemism: layoffs
        "operational streamlining",  # Euphemism: layoffs/cuts
        "workforce actions",  # Euphemism: layoffs
        "organizational actions",  # Euphemism: layoffs
        "headcount actions",  # Euphemism: layoffs
        "people costs",  # When discussing reductions
        "personnel reductions",  # Euphemism: layoffs
        "attrition",  # When accelerated = soft layoffs
        "hiring freeze",  # Precursor to problems
        "workforce reduction",  # Direct but softened
        "labor shortage",
        "workforce",
        "strike",
        "labor dispute",
        "work stoppage",
        "staffing",
        "talent challenges",  # Euphemism: labor shortage
        "labor availability",  # Euphemism: shortage
        "workforce dynamics",  # Euphemism: problems
        "retention challenges",  # Euphemism: turnover
        "competitive labor market",  # Euphemism: shortage
        "labor constraints",  # Euphemism: shortage
        "skilled labor",  # When discussing scarcity
        "hiring challenges",  # Euphemism: shortage
        "wage pressures",  # Indicates labor tightness
        "labor costs",  # When increasing
        "workforce availability",  # Euphemism: shortage
        "talent acquisition",  # When discussing difficulties
        "employee turnover",  # Direct but concerning
        "attrition rates",  # When elevated
        "labor relations",  # When discussing issues
        "union negotiations",  # Potential disruption
        "collective bargaining",  # Potential disruption
        "demand normalization",  # Euphemism: sales dropping
        "demand moderation",  # Euphemism: slowdown
        "soft demand",  # Euphemism: falling sales
        "demand softness",  # Euphemism: falling sales
        "volume declines",  # Direct but softened
        "market softness",  # Euphemism: recession/downturn
        "channel destocking",  # Euphemism: retailers not ordering
        "customer inventory adjustments",  # They overbought
        "order timing",  # Euphemism: orders delayed/cancelled
        "demand patterns",  # Euphemism: irregular demand
        "market maturation",  # Euphemism: growth slowing
        "category headwinds",  # Euphemism: products not selling
        "promotional environment",  # Euphemism: heavy discounting
        "competitive dynamics",  # Euphemism: losing share
        "market rationalization",  # Euphemism: shrinking market
        "demand environment",  # Euphemism: weak demand
        "consumption patterns",  # When discussing shifts
        "consumer behavior",  # When discussing concerns
        "discretionary spending",  # When weak
        "customer caution",  # Euphemism: not buying
        "purchase deferrals",  # Euphemism: customers waiting
        "elongated sales cycles",  # Euphemism: slow decisions
        "deal slippage",  # Euphemism: lost/delayed sales
        "pipeline conversion",  # When discussing problems
        "booking trends",  # When discussing concerns
        "order rates",  # When discussing declines
        "win rates",  # When discussing declines
        "underperformance",  # Euphemism: losses/failure
        "below expectations",  # Euphemism: failure
        "missed expectations",  # Direct but softened
        "challenging quarter",  # Euphemism: bad results
        "execution challenges",  # Euphemism: management failures
        "transitional period",  # Euphemism: struggling
        "reset",  # Euphemism: write-downs/restart
        "transformation",  # Euphemism: major problems
        "strategic review",  # Euphemism: considering selling
        "portfolio optimization",  # Euphemism: selling businesses
        "non-core",  # Euphemism: about to be sold
        "strategic alternatives",  # Euphemism: looking to sell
        "exploring options",  # Euphemism: desperate
        "addressing",  # Euphemism: have a problem
        "working through",  # Euphemism: dealing with problems
        "near-term pressures",  # Euphemism: current problems
        "short-term headwinds",  # Euphemism: current problems
        "temporary factors",  # Often not temporary
        "transitory impacts",  # Often not transitory
        "one-time items",  # Often not one-time
        "non-recurring charges",  # Often recurring
        "special items",  # Hiding bad news
        "unusual items",  # Hiding bad news
        "revised outlook",  # Euphemism: cutting forecast
        "updated guidance",  # Usually bad news
        "recalibrating expectations",  # Euphemism: cutting forecast
        "moderating our view",  # Euphemism: cutting forecast
        "adjusting expectations",  # Euphemism: cutting forecast
        "visibility",  # When lacking = can't predict
        "limited visibility",  # Euphemism: uncertainty
        "uncertain outlook",  # Euphemism: bad times ahead
        "cautiously optimistic",  # Euphemism: worried
        "measured optimism",  # Euphemism: worried
        "prudent approach",  # Euphemism: expecting problems
        "conservative assumptions",  # Expecting problems
        "range of outcomes",  # Euphemism: high uncertainty
        "scenario planning",  # Euphemism: preparing for bad
        "wider range",  # Euphemism: more uncertainty
        "macro uncertainty",  # Euphemism: economic concerns
        "evolving conditions",  # Euphemism: instability
        "capital discipline",  # Euphemism: cutting spending
        "investment prioritization",  # Euphemism: cutting spending
        "discretionary spending reduction",  # Cutting spending
        "capex moderation",  # Euphemism: cutting investment
        "prudent investment",  # Euphemism: not investing
        "selective investments",  # Euphemism: cutting most
        "investment optimization",  # Euphemism: cutting spending
        "disciplined approach",  # Euphemism: cutting spending
        "focused investments",  # Cutting elsewhere
        "capital allocation",  # When discussing constraints
        "spending controls",  # Cutting spending
        "budget constraints",  # Direct but softened
        "resource allocation",  # When discussing constraints
        "impairment",  # Write-down
        "goodwill impairment",  # Overpaid for acquisition
        "asset rationalization",  # Write-offs
        "balance sheet optimization",  # Often bad news
        "write-down",  # Direct
        "write-off",  # Direct
        "asset impairment",  # Write-down
        "carrying value",  # When discussing reduction
        "valuation adjustments",  # Euphemism: write-downs
        "fair value adjustments",  # Often negative
        "reserve build",  # Expecting losses
        "provision increase",  # Expecting losses
        "allowance increase",  # Expecting losses
        "charge",  # Often negative
        "accrual",  # When discussing problems
        "quality event",  # Euphemism: defect/recall
        "product issue",  # Euphemism: defect
        "voluntary recall",  # Still a recall
        "field action",  # Euphemism: recall
        "customer notification",  # Euphemism: recall
        "remediation",  # Euphemism: fixing problems
        "process improvement",  # Euphemism: had quality issues
        "enhanced protocols",  # Euphemism: had safety issues
        "corrective action",  # Fixing problems
        "quality improvement",  # Had quality issues
        "product quality",  # When discussing issues
        "manufacturing quality",  # When discussing issues
        "compliance",  # When discussing issues
        "regulatory compliance",  # When discussing issues
        "tariff",
        "tariffs",
        "sanctions",
        "trade restrictions",
        "export controls",
        "import restrictions",
        "trade war",
        "customs",
        "duties",
        "geopolitical uncertainty",  # Euphemism: trade risk
        "trade dynamics",  # Euphemism: trade problems
        "regulatory environment",  # Euphemism: restrictions
        "policy changes",  # Euphemism: negative policies
        "trade tensions",  # Euphemism: trade war
        "geopolitical risk",
        "country risk",
        "political instability",
        "trade policy",
        "import duties",
        "export restrictions",
        "regional dynamics",  # Euphemism: instability
        "local market conditions",  # Euphemism: problems
        "emerging market",  # When discussing risks
        "currency",  # When discussing headwinds
        "foreign exchange",  # When discussing headwinds
        "fx headwinds",  # Currency losses
        "translation impact",  # Currency losses
        "transactional impact",  # Currency losses
        "reshoring",
        "nearshoring",
        "onshoring",
        "friend-shoring",
        "supply chain resilience",
        "diversification",
        "de-risking",
        "supply chain transformation",  # Indicates prior problems
        "supply chain redesign",  # Indicates prior problems
        "supply chain reconfiguration",  # Indicates prior problems
        "strategic inventory",  # Indicates supply concerns
        "safety stock build",  # Indicates supply concerns
        "regionalization",  # Response to global issues
        "localization",  # Response to global issues
        "supplier base diversification",  # Had concentration risk
        "geographic diversification",  # Had concentration risk
        "product rationalization",  # Euphemism: killing products
        "SKU optimization",  # Euphemism: cutting products
        "product lifecycle",  # When discussing end
        "sunset",  # Killing a product
        "end of life",  # Discontinuing product
        "legacy systems",  # Old, problematic tech
        "technical debt",  # Accumulated problems
        "platform migration",  # Usually painful
        "system modernization",  # Old systems problematic
        "product simplification",  # Cutting products
        "portfolio rationalization",  # Cutting products
        "R&D prioritization",  # Cutting projects
        "development delays",  # Product delays
        "launch delays",  # Product delays
        # =======================================================================
        # REAL ESTATE/FACILITIES - Euphemistic
        # =======================================================================
        "footprint optimization",  # Euphemism: closing locations
        "real estate rationalization",  # Closing locations
        "store optimization",  # Closing stores
        "location consolidation",  # Closing locations
        "space reduction",  # Closing/shrinking
        "facility consolidation",  # Closing facilities
        "site rationalization",  # Closing sites
        "lease optimization",  # Closing locations
        "branch optimization",  # Closing branches
        "network optimization",  # Closing locations
        "semiconductor",
        "semiconductors",
        "chip shortage",
        "component shortage",
        "chips",
        "integrated circuits",
        "wafers",
        "rare earth",
        "rare earths",
        "lithium",
        "cobalt",
        "nickel",
        "copper",
        "aluminum",
        "steel",
        "resin",
        "plastic",
        "battery materials",
        "critical minerals",
        "natural gas",
        "crude oil",
        "energy costs",
        "power costs",
        "electricity costs",
        "fuel costs",
        "packaging materials",
        "packaging costs",
        # =======================================================================
        # PORT/SHIPPING SPECIFIC
        # =======================================================================
        "port congestion",
        "dock congestion",
        "terminal congestion",
        "container shortage",
        "vessel availability",
        "ocean freight",
        "air freight",
        "trucking",
        "rail capacity",
        "intermodal",
        "carrier capacity",
        "shipping capacity",
        "freight rates",
        "spot rates",
        "contract rates",
        "modest impact",  # Trying to minimize
        "limited impact",  # Trying to minimize
        "manageable",  # Trying to minimize
        "contained",  # Trying to minimize
        "isolated",  # Trying to minimize
        "limited exposure",  # Trying to minimize
        "de minimis",  # Trying to minimize
        "immaterial",  # Trying to minimize
        "not significant",  # Trying to minimize
        "minor",  # Trying to minimize
        "proactive measures",  # Euphemism: reacting to problems
        "getting ahead of",  # Euphemism: reacting to problems
        "well positioned",  # Trying to reassure
        "positioned well",  # Trying to reassure
        "taking action",  # Addressing problems
        "addressing proactively",  # Reacting to problems
        "actively managing",  # Dealing with issues
        "monitoring closely",  # Concerned
        "watching carefully",  # Concerned
        "headwinds",  # Generic euphemism: problems
        "challenges",  # Generic euphemism: problems
        "uncertainty",  # Generic euphemism: risk
        "volatility",  # Generic euphemism: instability
        "pressures",  # Generic euphemism: problems
        "difficult environment",  # Euphemism: bad conditions
        "challenging conditions",  # Euphemism: problems
        "evolving situation",  # Euphemism: ongoing problems
        "fluid situation",  # Euphemism: uncertainty
        "dynamic environment",  # Euphemism: instability
        "complex environment",  # Euphemism: problems
        "unprecedented",  # Euphemism: major problems
        "extraordinary",  # Euphemism: major problems
        "unforeseen",  # Euphemism: didn't plan for
        "unanticipated",  # Euphemism: didn't plan for
        "unexpected",  # Euphemism: didn't plan for
        "difficult decisions",  # Euphemism: bad news coming
        "tough decisions",  # Euphemism: bad news coming
        "hard choices",  # Euphemism: bad news coming
        "near-term",  # Euphemism: current problems
        "short-term",  # Euphemism: current problems
        "temporary",  # Often not temporary
        "transitory",  # Often not transitory
        "as we navigate",  # Facing difficulties
        "navigating",  # Facing difficulties
        "working through",  # Dealing with problems
        "managing through",  # Dealing with problems
    ]

    # Sectors to track (SIC code ranges)
    SECTORS_OF_INTEREST = {
        "manufacturing": (2000, 3999),
        "transportation": (4000, 4999),
        "retail": (5200, 5999),
        "technology": (3570, 3579),  # Computer equipment
    }

    def __init__(self):
        """Initialize the SEC EDGAR collector."""
        super().__init__(name="SEC EDGAR", source_id="sec_edgar")
        self.settings = get_settings()
        self.rate_limiter = get_rate_limiter("sec_edgar")

        # User-Agent is required by SEC
        self._user_agent = self.settings.sec_user_agent

    async def _get_monitored_ciks(self) -> list[str]:
        """Get list of company CIKs to monitor from configuration or database.

        Returns:
            List of CIK strings, or empty list if not configured
        """
        # Check settings for configured CIKs
        if hasattr(self.settings, 'sec_monitored_ciks') and self.settings.sec_monitored_ciks:
            return self.settings.sec_monitored_ciks

        # Would query database for configured companies
        # For now, return empty list to indicate configuration is needed
        self.logger.warning(
            "SEC EDGAR monitored CIKs not configured. "
            "Set sec_monitored_ciks in settings or configure via database."
        )
        return []

    @property
    def frequency(self) -> DataFrequency:
        """SEC filings are collected daily."""
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        """Return cron schedule - daily at midnight UTC (7 PM ET)."""
        return "0 0 * * 1-5"  # Weekdays only

    def get_default_series(self) -> list[str]:
        """Return default series to collect."""
        return ["SEC_FILINGS_10K", "SEC_FILINGS_10Q", "SEC_FILINGS_8K"]

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect SEC filings and extract supply chain signals.

        Args:
            series_ids: Optional list of series IDs to collect
            start_date: Start date for filings (default: yesterday)
            end_date: End date for filings (default: today)

        Returns:
            List of DataPoint objects with filing signals
        """
        if start_date is None:
            start_date = datetime.now(UTC) - timedelta(days=1)
        if end_date is None:
            end_date = datetime.now(UTC)

        data_points: list[DataPoint] = []

        async with httpx.AsyncClient(
            timeout=60.0,
            headers={"User-Agent": self._user_agent},
        ) as client:
            for filing_type in self.FILING_TYPES:
                self.logger.info(
                    "Collecting filings",
                    filing_type=filing_type,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                )

                try:
                    filings = await self._fetch_recent_filings(
                        client, filing_type, start_date, end_date
                    )

                    for filing in filings:
                        # Extract signals from filing
                        signals = await self._extract_signals(client, filing)

                        for signal in signals:
                            dp = self._signal_to_datapoint(signal)
                            data_points.append(dp)

                except Exception as e:
                    self.logger.error(
                        "Failed to collect filings",
                        filing_type=filing_type,
                        error=str(e),
                    )
                    continue

        self.logger.info(
            "Collection complete",
            total_signals=len(data_points),
        )

        return data_points

    async def _fetch_recent_filings(
        self,
        client: httpx.AsyncClient,
        filing_type: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[SECFiling]:
        """Fetch recent filings of a specific type.

        Uses the SEC full-text search API to find filings.

        Args:
            client: HTTP client
            filing_type: Form type (10-K, 10-Q, 8-K)
            start_date: Start date
            end_date: End date

        Returns:
            List of SECFiling objects
        """
        filings: list[SECFiling] = []

        keyword_query = " OR ".join(f'"{kw}"' for kw in self.SUPPLY_CHAIN_KEYWORDS[:5])
        search_url = f"{self.EFTS_URL}?q={keyword_query}&dateRange=custom"
        search_url += f"&startdt={start_date.strftime('%Y-%m-%d')}"
        search_url += f"&enddt={end_date.strftime('%Y-%m-%d')}"
        search_url += f"&forms={filing_type}"

        try:
            async with self.rate_limiter:
                response = await client.get(search_url)

            if response.status_code == 200:
                data = response.json()
                hits = data.get("hits", {}).get("hits", [])

                for hit in hits[:50]:  # Limit to 50 per type
                    source = hit.get("_source", {})
                    filing = self._parse_filing_hit(source)
                    if filing:
                        filings.append(filing)

        except httpx.HTTPStatusError as e:
            self.logger.warning(
                "EFTS search failed, falling back to submissions API",
                status_code=e.response.status_code,
            )
            # Fallback: use submissions API for recent filings
            filings = await self._fetch_from_submissions_api(
                client, filing_type, start_date, end_date
            )

        except Exception as e:
            self.logger.error("Failed to fetch filings", error=str(e))

        return filings

    async def _fetch_from_submissions_api(
        self,
        client: httpx.AsyncClient,
        filing_type: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[SECFiling]:
        """Fallback method using submissions API for specific CIKs.

        Fetches recent filings from a list of major companies.

        Args:
            client: HTTP client
            filing_type: Form type
            start_date: Start date
            end_date: End date

        Returns:
            List of SECFiling objects
        """
        filings: list[SECFiling] = []

        major_ciks = await self._get_monitored_ciks()

        if not major_ciks:
            self.logger.warning(
                "No company CIKs configured for SEC EDGAR monitoring. "
                "Configure via settings or database."
            )
            return filings

        for cik in major_ciks:
            try:
                async with self.rate_limiter:
                    url = f"{self.BASE_URL}/submissions/CIK{cik}.json"
                    response = await client.get(url)

                if response.status_code != 200:
                    continue

                data = response.json()
                company_name = data.get("name", "")
                tickers = data.get("tickers", [])
                ticker = tickers[0] if tickers else None
                sic = data.get("sic", "")
                sic_desc = data.get("sicDescription", "")

                recent = data.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                dates = recent.get("filingDate", [])
                accessions = recent.get("accessionNumber", [])
                primary_docs = recent.get("primaryDocument", [])

                for i, form in enumerate(forms):
                    if form != filing_type:
                        continue

                    filing_date = datetime.strptime(dates[i], "%Y-%m-%d")
                    if not (start_date <= filing_date <= end_date):
                        continue

                    accession = accessions[i].replace("-", "")
                    primary_doc = primary_docs[i]
                    file_url = f"{self.BASE_URL}/Archives/edgar/data/{cik}/{accession}/{primary_doc}"

                    filing = SECFiling(
                        cik=cik,
                        company_name=company_name,
                        ticker=ticker,
                        filing_type=form,
                        filing_date=filing_date,
                        accession_number=accessions[i],
                        primary_document=primary_doc,
                        file_url=file_url,
                        sic_code=sic,
                        sic_description=sic_desc,
                    )
                    filings.append(filing)

            except Exception as e:
                self.logger.debug("Failed to fetch CIK", cik=cik, error=str(e))
                continue

        return filings

    def _parse_filing_hit(self, source: dict[str, Any]) -> SECFiling | None:
        """Parse a filing from EFTS search hit.

        Args:
            source: Source data from EFTS hit

        Returns:
            SECFiling or None if parsing fails
        """
        try:
            cik = source.get("ciks", [""])[0]
            company_names = source.get("display_names", source.get("names", []))
            company_name = company_names[0] if company_names else ""
            tickers = source.get("tickers", [])

            filing_date_str = source.get("file_date", source.get("filing_date", ""))
            if filing_date_str:
                filing_date = datetime.strptime(filing_date_str[:10], "%Y-%m-%d")
            else:
                return None

            return SECFiling(
                cik=cik,
                company_name=company_name,
                ticker=tickers[0] if tickers else None,
                filing_type=source.get("form", ""),
                filing_date=filing_date,
                accession_number=source.get("accession_number", ""),
                primary_document=source.get("file_name", ""),
                file_url=source.get("file_url", ""),
            )

        except Exception as e:
            self.logger.debug("Failed to parse filing hit", error=str(e))
            return None

    async def _extract_signals(
        self,
        client: httpx.AsyncClient,
        filing: SECFiling,
    ) -> list[FilingSignal]:
        """Extract supply chain signals from a filing.

        Args:
            client: HTTP client
            filing: The filing to analyze

        Returns:
            List of FilingSignal objects
        """
        signals: list[FilingSignal] = []

        # For 8-K filings, we don't need to fetch full text
        # Just record the filing as a potential signal
        if filing.filing_type == "8-K":
            signal = FilingSignal(
                filing=filing,
                signal_type="event_filing",
                extracted_text=f"8-K filing from {filing.company_name}",
                section="8-K",
            )
            signals.append(signal)
            return signals

        # For 10-K and 10-Q, try to fetch and analyze the filing
        try:
            if not filing.file_url:
                return signals

            async with self.rate_limiter:
                response = await client.get(filing.file_url)

            if response.status_code != 200:
                return signals

            content = response.text

            risk_factors = self._extract_risk_factors(content)
            if risk_factors:
                keyword_matches = self._count_keyword_matches(risk_factors)

                if sum(keyword_matches.values()) > 0:
                    signal = FilingSignal(
                        filing=filing,
                        signal_type="risk_factor_keywords",
                        extracted_text=risk_factors[:5000],  # Limit size
                        keyword_matches=keyword_matches,
                        section="risk_factors",
                    )
                    signals.append(signal)

            mda = self._extract_mda(content)
            if mda:
                keyword_matches = self._count_keyword_matches(mda)

                if sum(keyword_matches.values()) > 0:
                    signal = FilingSignal(
                        filing=filing,
                        signal_type="mda_keywords",
                        extracted_text=mda[:5000],
                        keyword_matches=keyword_matches,
                        section="mda",
                    )
                    signals.append(signal)

        except Exception as e:
            self.logger.debug(
                "Failed to extract signals",
                filing=filing.accession_number,
                error=str(e),
            )

        return signals

    def _extract_risk_factors(self, content: str) -> str:
        """Extract risk factors section from filing HTML/text.

        Args:
            content: Filing content

        Returns:
            Risk factors text or empty string
        """
        patterns = [
            r"(?i)Item\s*1A[.\s]*Risk\s*Factors(.*?)(?=Item\s*1B|Item\s*2|$)",
            r"(?i)RISK\s*FACTORS(.*?)(?=UNRESOLVED\s*STAFF|PROPERTIES|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                text = match.group(1)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 100:  # Minimum viable content
                    return text[:50000]  # Limit size

        return ""

    def _extract_mda(self, content: str) -> str:
        """Extract MD&A section from filing.

        Args:
            content: Filing content

        Returns:
            MD&A text or empty string
        """
        patterns = [
            r"(?i)Item\s*7[.\s]*Management.*?Discussion.*?Analysis(.*?)(?=Item\s*7A|Item\s*8|$)",
            r"(?i)MANAGEMENT.*?DISCUSSION.*?ANALYSIS(.*?)(?=QUANTITATIVE|FINANCIAL\s*STATEMENTS|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                text = match.group(1)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 100:
                    return text[:50000]

        return ""

    def _count_keyword_matches(self, text: str) -> dict[str, int]:
        """Count occurrences of supply chain keywords in text.

        Args:
            text: Text to analyze

        Returns:
            Dict mapping keyword to count
        """
        text_lower = text.lower()
        matches: dict[str, int] = {}

        for keyword in self.SUPPLY_CHAIN_KEYWORDS:
            count = text_lower.count(keyword.lower())
            if count > 0:
                matches[keyword] = count

        return matches

    def _signal_to_datapoint(self, signal: FilingSignal) -> DataPoint:
        """Convert a FilingSignal to a DataPoint.

        Args:
            signal: The filing signal

        Returns:
            DataPoint object
        """
        sector = None
        if signal.filing.sic_code:
            try:
                sic = int(signal.filing.sic_code)
                for sector_name, (low, high) in self.SECTORS_OF_INTEREST.items():
                    if low <= sic <= high:
                        sector = sector_name
                        break
            except ValueError:
                pass

        total_matches = sum(signal.keyword_matches.values())
        signal_strength = min(1.0, total_matches / 50)  # Normalize to 0-1

        series_id = f"SEC_{signal.filing.filing_type}_{signal.signal_type.upper()}"

        return DataPoint(
            source_id=self.source_id,
            series_id=series_id,
            timestamp=signal.filing.filing_date,
            value=signal_strength,
            value_text=signal.extracted_text[:1000] if signal.extracted_text else None,
            metadata={
                "cik": signal.filing.cik,
                "company_name": signal.filing.company_name,
                "ticker": signal.filing.ticker,
                "filing_type": signal.filing.filing_type,
                "accession_number": signal.filing.accession_number,
                "signal_type": signal.signal_type,
                "section": signal.section,
                "keyword_matches": signal.keyword_matches,
                "total_keyword_matches": total_matches,
                "sector": sector,
                "sic_code": signal.filing.sic_code,
                "file_url": signal.filing.file_url,
            },
        )

    async def validate_api_key(self) -> bool:
        """Validate SEC EDGAR access (no API key required, but user-agent is).

        Returns:
            True if API is accessible
        """
        try:
            async with httpx.AsyncClient(
                timeout=30.0,
                headers={"User-Agent": self._user_agent},
            ) as client:
                async with self.rate_limiter:
                    response = await client.get(
                        f"{self.BASE_URL}/submissions/CIK0000320193.json"
                    )
                return response.status_code == 200

        except Exception as e:
            self.logger.error("SEC EDGAR validation failed", error=str(e))
            return False


def get_sec_edgar_collector() -> SECEdgarCollector:
    """Get a SEC EDGAR collector instance."""
    return SECEdgarCollector()
