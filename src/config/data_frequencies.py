"""Data source frequency and retention policy configuration.

This module defines:
1. Publication frequencies for all data sources
2. Collection schedules (how often we check for new data)
3. Retention policies (how long to keep data before archiving/deleting)

Retention Policy Rules:
- Daily data: Keep 1 year of raw data, OR until 250 GB threshold hit
- Weekly data: Keep 2 years of raw data
- Monthly data: Keep 5 years of raw data
- Quarterly data: Keep 8 years of raw data
- Annual data: Keep 8 years of raw data
- All data older than retention period is compressed and archived
- Archives follow 8-year retention for compliance
"""

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any


class DataFrequency(str, Enum):
    """Publication frequency of data sources."""

    REAL_TIME = "real_time"  # Continuous/as-filed
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    IRREGULAR = "irregular"  # No fixed schedule (e.g., MARAD)


@dataclass(frozen=True)
class RetentionPolicy:
    """Retention policy for a data frequency tier."""

    frequency: DataFrequency
    raw_retention_days: int  # How long to keep uncompressed raw data
    archive_retention_years: int  # How long to keep compressed archives
    size_threshold_gb: float | None  # Optional size-based trigger for archival
    description: str

    @property
    def raw_retention_timedelta(self) -> timedelta:
        """Get raw retention as timedelta."""
        return timedelta(days=self.raw_retention_days)


# Retention policies by frequency tier
RETENTION_POLICIES: dict[DataFrequency, RetentionPolicy] = {
    DataFrequency.REAL_TIME: RetentionPolicy(
        frequency=DataFrequency.REAL_TIME,
        raw_retention_days=365,  # 1 year
        archive_retention_years=8,
        size_threshold_gb=250.0,  # Archive if exceeds 250 GB
        description="Real-time/as-filed data: 1 year raw, 8 years archived, or 250 GB threshold",
    ),
    DataFrequency.DAILY: RetentionPolicy(
        frequency=DataFrequency.DAILY,
        raw_retention_days=365,  # 1 year
        archive_retention_years=8,
        size_threshold_gb=250.0,  # Archive if exceeds 250 GB
        description="Daily data: 1 year raw, 8 years archived, or 250 GB threshold",
    ),
    DataFrequency.WEEKLY: RetentionPolicy(
        frequency=DataFrequency.WEEKLY,
        raw_retention_days=730,  # 2 years
        archive_retention_years=8,
        size_threshold_gb=None,  # No size threshold
        description="Weekly data: 2 years raw, 8 years archived",
    ),
    DataFrequency.MONTHLY: RetentionPolicy(
        frequency=DataFrequency.MONTHLY,
        raw_retention_days=1825,  # 5 years
        archive_retention_years=8,
        size_threshold_gb=None,
        description="Monthly data: 5 years raw, 8 years archived",
    ),
    DataFrequency.QUARTERLY: RetentionPolicy(
        frequency=DataFrequency.QUARTERLY,
        raw_retention_days=2920,  # 8 years
        archive_retention_years=8,
        size_threshold_gb=None,
        description="Quarterly data: 8 years raw, 8 years archived",
    ),
    DataFrequency.ANNUAL: RetentionPolicy(
        frequency=DataFrequency.ANNUAL,
        raw_retention_days=2920,  # 8 years
        archive_retention_years=8,
        size_threshold_gb=None,
        description="Annual data: 8 years raw, 8 years archived",
    ),
    DataFrequency.IRREGULAR: RetentionPolicy(
        frequency=DataFrequency.IRREGULAR,
        raw_retention_days=365,  # Keep latest + 1 year of prior versions
        archive_retention_years=8,
        size_threshold_gb=None,
        description="Irregular data: Archive old when new arrives, keep 8 years",
    ),
}


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""

    source_id: str
    name: str
    publication_frequency: DataFrequency
    collection_schedule: str  # Cron expression or interval
    description: str
    api_available: bool = True
    notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def retention_policy(self) -> RetentionPolicy:
        """Get the retention policy for this source."""
        return RETENTION_POLICIES[self.publication_frequency]


# =============================================================================
# DATA SOURCE CONFIGURATIONS
# Organized by publication frequency for clarity
# =============================================================================

DATA_SOURCES: dict[str, DataSourceConfig] = {
    # =========================================================================
    # DAILY DATA SOURCES
    # Collection: Multiple times daily or once daily
    # Retention: 1 year raw, 8 years archived, OR 250 GB threshold
    # =========================================================================
    "baltic_dry_index": DataSourceConfig(
        source_id="baltic_dry_index",
        name="Baltic Dry Index (via FRED)",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 18 * * 1-5",  # 6 PM UTC weekdays (after London close)
        description="Dry bulk shipping rates composite index",
        notes="Includes BDI, BCIY (Capesize), BPIY (Panamax)",
    ),
    "lme_warehouse_stocks": DataSourceConfig(
        source_id="lme_warehouse_stocks",
        name="LME Warehouse Stocks",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 10 * * 1-5",  # 10 AM UTC (after 09:00 London publish)
        description="London Metal Exchange warehouse inventory levels",
        notes="Copper, aluminum, zinc, nickel, lead, tin",
    ),
    "comex_warehouse_stocks": DataSourceConfig(
        source_id="comex_warehouse_stocks",
        name="COMEX Warehouse Stocks",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 22 * * 1-5",  # 10 PM UTC (after NY close)
        description="COMEX registered and eligible inventory",
        notes="Gold, silver, copper",
    ),
    "freightos_fbx": DataSourceConfig(
        source_id="freightos_fbx",
        name="Freightos Baltic Index (FBX)",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 14 * * 1-5",  # 2 PM UTC
        description="Container shipping rates across 13 lanes",
    ),
    "sec_edgar": DataSourceConfig(
        source_id="sec_edgar",
        name="SEC EDGAR Filings",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 3 * * *",  # 3 AM UTC (after overnight processing)
        description="10-K, 10-Q, 8-K filings",
        notes="8-K is real-time, but we batch collect daily",
    ),
    "usaspending": DataSourceConfig(
        source_id="usaspending",
        name="USASpending Contract Awards",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 6 * * *",  # 6 AM UTC
        description="Federal contract awards and obligations",
    ),
    "faa_registry": DataSourceConfig(
        source_id="faa_registry",
        name="FAA Aircraft Registry",
        publication_frequency=DataFrequency.DAILY,
        collection_schedule="0 7 * * *",  # 7 AM UTC
        description="Aircraft registration and production data",
    ),
    # =========================================================================
    # WEEKLY DATA SOURCES
    # Collection: Once per week, timed to publication schedule
    # Retention: 2 years raw, 8 years archived
    # =========================================================================
    "eia_petroleum": DataSourceConfig(
        source_id="eia_petroleum",
        name="EIA Weekly Petroleum Status",
        publication_frequency=DataFrequency.WEEKLY,
        collection_schedule="30 15 * * 3",  # Wednesday 3:30 PM UTC (10:30 AM ET)
        description="Crude inventories, refinery utilization, SPR levels",
    ),
    "eia_natural_gas": DataSourceConfig(
        source_id="eia_natural_gas",
        name="EIA Natural Gas Storage",
        publication_frequency=DataFrequency.WEEKLY,
        collection_schedule="30 15 * * 4",  # Thursday 3:30 PM UTC (10:30 AM ET)
        description="Underground natural gas storage by region",
    ),
    "aisi_steel": DataSourceConfig(
        source_id="aisi_steel",
        name="AISI Weekly Steel Production",
        publication_frequency=DataFrequency.WEEKLY,
        collection_schedule="0 18 * * 1",  # Monday 6 PM UTC
        description="Raw steel production and capacity utilization",
    ),
    "drewry_wci": DataSourceConfig(
        source_id="drewry_wci",
        name="Drewry World Container Index",
        publication_frequency=DataFrequency.WEEKLY,
        collection_schedule="0 10 * * 4",  # Thursday 10 AM UTC
        description="Container shipping rates",
    ),
    "flexport_oti": DataSourceConfig(
        source_id="flexport_oti",
        name="Flexport Ocean Timeliness Indicator",
        publication_frequency=DataFrequency.WEEKLY,
        collection_schedule="0 14 * * 1",  # Monday 2 PM UTC
        description="Ocean shipping transit times Asia-to-West",
    ),
    "warn_notices": DataSourceConfig(
        source_id="warn_notices",
        name="WARN Act Layoff Notices",
        publication_frequency=DataFrequency.WEEKLY,
        collection_schedule="0 8 * * 3",  # Wednesday 8 AM UTC
        description="State-level layoff notices (real-time source, weekly collection)",
        notes="Real-time at source, but we batch collect weekly",
    ),
    # =========================================================================
    # MONTHLY DATA SOURCES
    # Collection: Timed to release schedules
    # Retention: 5 years raw, 8 years archived
    # =========================================================================
    "census_m3": DataSourceConfig(
        source_id="census_m3",
        name="Census M3 Manufacturers Survey",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 15 4 * *",  # 4th of month, 3 PM UTC
        description="Shipments, inventories, orders",
    ),
    "port_la_teu": DataSourceConfig(
        source_id="port_la_teu",
        name="Port of LA/Long Beach TEU",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 18 16 * *",  # 16th of month, 6 PM UTC
        description="Container throughput for West Coast ports",
    ),
    "acc_cab": DataSourceConfig(
        source_id="acc_cab",
        name="ACC Chemical Activity Barometer",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 14 L * *",  # Last day of month, 2 PM UTC
        description="Chemical industry production and demand",
        notes="L = last day of month in cron (requires special handling)",
    ),
    "afpa_paper": DataSourceConfig(
        source_id="afpa_paper",
        name="AF&PA Paper/Packaging Statistics",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 14 20 * *",  # 20th of month, 2 PM UTC
        description="Printing-writing and packaging paper shipments",
    ),
    "semi_billings": DataSourceConfig(
        source_id="semi_billings",
        name="SEMI Equipment Billings",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 16 21 * *",  # 21st of month, 4 PM UTC
        description="Semiconductor equipment billings North America",
    ),
    "census_trade": DataSourceConfig(
        source_id="census_trade",
        name="Census Trade Data (HS Chapter 88)",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 13 7 * *",  # 7th of month, 1 PM UTC
        description="Aircraft, spacecraft, and parts trade",
    ),
    "bls_employment": DataSourceConfig(
        source_id="bls_employment",
        name="BLS Employment Situation",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="30 13 1-10 * *",  # First Friday, 8:30 AM ET
        description="Nonfarm payrolls, unemployment rate",
    ),
    "eia_lng": DataSourceConfig(
        source_id="eia_lng",
        name="EIA LNG Export Terminal Data",
        publication_frequency=DataFrequency.MONTHLY,
        collection_schedule="0 16 L * *",  # Last day of month, 4 PM UTC
        description="LNG export capacity utilization",
    ),
    "opensecrets": DataSourceConfig(
        source_id="opensecrets",
        name="OpenSecrets Lobbying Data",
        publication_frequency=DataFrequency.QUARTERLY,
        collection_schedule="0 12 20 1,4,7,10 *",  # 20th of Jan, Apr, Jul, Oct
        description="Federal lobbying disclosures",
        notes="Quarterly filings, collected after deadline",
    ),
    # =========================================================================
    # QUARTERLY DATA SOURCES
    # Collection: After quarterly release dates
    # Retention: 8 years raw, 8 years archived
    # =========================================================================
    "usda_grain_stocks": DataSourceConfig(
        source_id="usda_grain_stocks",
        name="USDA Grain Stocks",
        publication_frequency=DataFrequency.QUARTERLY,
        collection_schedule="0 17 L 3,6,9,12 *",  # End of Mar, Jun, Sep, Dec
        description="On-farm and off-farm grain inventory",
    ),
    "fmc_container_stats": DataSourceConfig(
        source_id="fmc_container_stats",
        name="FMC Containerized Freight Statistics",
        publication_frequency=DataFrequency.QUARTERLY,
        collection_schedule="0 14 15 1,4,7,10 *",  # 15th of Jan, Apr, Jul, Oct
        description="Container dwell times and throughput",
    ),
    "bea_gdp": DataSourceConfig(
        source_id="bea_gdp",
        name="BEA GDP and Motor Vehicle Output",
        publication_frequency=DataFrequency.QUARTERLY,
        collection_schedule="0 13 L 1,4,7,10 *",  # End of Jan, Apr, Jul, Oct
        description="GDP estimates including motor vehicle output",
    ),
    "semi_smm": DataSourceConfig(
        source_id="semi_smm",
        name="SEMI Semiconductor Manufacturing Monitor",
        publication_frequency=DataFrequency.QUARTERLY,
        collection_schedule="0 14 15 1,4,7,10 *",  # 15th of Jan, Apr, Jul, Oct
        description="Comprehensive semiconductor supply chain data",
    ),
    "afpa_boxboard": DataSourceConfig(
        source_id="afpa_boxboard",
        name="AF&PA Boxboard/Containerboard",
        publication_frequency=DataFrequency.QUARTERLY,
        collection_schedule="0 14 20 1,4,7,10 *",  # 20th of Jan, Apr, Jul, Oct
        description="Packaging production and operating rates",
    ),
    # =========================================================================
    # ANNUAL DATA SOURCES
    # Collection: After annual publication
    # Retention: 8 years raw, 8 years archived
    # =========================================================================
    "bea_io_tables": DataSourceConfig(
        source_id="bea_io_tables",
        name="BEA Input-Output Tables",
        publication_frequency=DataFrequency.ANNUAL,
        collection_schedule="0 14 1 10 *",  # October 1st (after late-Sep release)
        description="Inter-industry dependency matrices",
    ),
    "usgs_minerals": DataSourceConfig(
        source_id="usgs_minerals",
        name="USGS Mineral Commodity Summaries",
        publication_frequency=DataFrequency.ANNUAL,
        collection_schedule="0 14 1 2 *",  # February 1st (after late-Jan release)
        description="Production and trade for 90+ minerals",
    ),
    "iea_critical_minerals": DataSourceConfig(
        source_id="iea_critical_minerals",
        name="IEA Critical Minerals Outlook",
        publication_frequency=DataFrequency.ANNUAL,
        collection_schedule="0 14 1 6 *",  # June 1st (after May release)
        description="Global critical minerals supply/demand outlook",
    ),
    # =========================================================================
    # IRREGULAR DATA SOURCES
    # Collection: Check daily, archive old when new arrives
    # Retention: 1 year of prior versions, 8 years archived
    # =========================================================================
    "marad_shipyard": DataSourceConfig(
        source_id="marad_shipyard",
        name="MARAD Shipyard Data",
        publication_frequency=DataFrequency.IRREGULAR,
        collection_schedule="0 12 * * *",  # Check daily at noon UTC
        description="U.S. shipbuilding and repair facility survey",
        notes="No fixed schedule; check daily for updates, archive old when new arrives",
    ),
}


def get_data_source(source_id: str) -> DataSourceConfig | None:
    """Get configuration for a data source."""
    return DATA_SOURCES.get(source_id)


def get_sources_by_frequency(frequency: DataFrequency) -> list[DataSourceConfig]:
    """Get all data sources with a given publication frequency."""
    return [s for s in DATA_SOURCES.values() if s.publication_frequency == frequency]


def get_daily_sources() -> list[DataSourceConfig]:
    """Get all daily data sources (for size-based archival monitoring)."""
    return get_sources_by_frequency(DataFrequency.DAILY)


def get_all_collection_schedules() -> dict[str, str]:
    """Get collection schedules for all data sources."""
    return {s.source_id: s.collection_schedule for s in DATA_SOURCES.values()}
