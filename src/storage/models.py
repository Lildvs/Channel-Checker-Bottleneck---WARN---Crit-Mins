"""SQLAlchemy ORM models for the database.

NAMING CONVENTION:
    SQLAlchemy reserves 'metadata' as an internal attribute on all ORM models.
    Use 'extra_data' instead of 'metadata' for JSONB columns storing arbitrary
    key-value data. Dataclass fields can still use 'metadata' - only ORM models
    are affected by this restriction.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class DataPointModel(Base):
    """Model for time-series data points."""

    __tablename__ = "data_points"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    source_id: Mapped[str] = mapped_column(String(50), nullable=False)
    series_id: Mapped[str] = mapped_column(String(100), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    quality_score: Mapped[float] = mapped_column(Float, default=1.0)
    is_preliminary: Mapped[bool] = mapped_column(Boolean, default=False)
    revision_number: Mapped[int] = mapped_column(Integer, default=0)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        Index("idx_data_points_source_series", "source_id", "series_id", "timestamp"),
        Index("idx_data_points_series_id", "series_id", "timestamp"),
    )


class BottleneckSignal(Base):
    """Model for detected bottleneck signals."""

    __tablename__ = "bottleneck_signals"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    subcategory: Mapped[str | None] = mapped_column(String(50), nullable=True)
    severity: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    affected_sectors: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    affected_commodities: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    source_series: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    status: Mapped[str] = mapped_column(String(20), default="active")
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint("severity >= 0 AND severity <= 1", name="check_severity_range"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="check_confidence_range"),
        CheckConstraint(
            "status IN ('active', 'resolved', 'false_positive', 'monitoring')",
            name="check_valid_status",
        ),
        Index("idx_bottleneck_signals_status", "status", "detected_at"),
        Index("idx_bottleneck_signals_category", "category", "detected_at"),
    )


class Sector(Base):
    """Model for sector definitions."""

    __tablename__ = "sectors"

    code: Mapped[str] = mapped_column(String(20), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_code: Mapped[str | None] = mapped_column(
        String(20), ForeignKey("sectors.code"), nullable=True
    )
    classification_system: Mapped[str] = mapped_column(String(20), default="NAICS")
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    # Self-referential relationship for parent-child sectors
    parent = relationship("Sector", remote_side=[code], backref="children")


class SectorDependency(Base):
    """Model for sector dependency relationships."""

    __tablename__ = "sector_dependencies"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    upstream_sector: Mapped[str] = mapped_column(String(20), nullable=False)
    downstream_sector: Mapped[str] = mapped_column(String(20), nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False)
    dependency_type: Mapped[str] = mapped_column(String(20), default="supply")
    source: Mapped[str] = mapped_column(String(50), default="BEA_IO")
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint("weight >= 0 AND weight <= 1", name="check_weight_range"),
        UniqueConstraint("upstream_sector", "downstream_sector", "source", "year"),
        Index("idx_sector_deps_upstream", "upstream_sector"),
        Index("idx_sector_deps_downstream", "downstream_sector"),
    )


class SeriesMetadataModel(Base):
    """Model for data series metadata."""

    __tablename__ = "series_metadata"

    series_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    frequency: Mapped[str | None] = mapped_column(String(20), nullable=True)
    seasonal_adjustment: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sector_codes: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    last_updated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)


class Anomaly(Base):
    """Model for detected anomalies."""

    __tablename__ = "anomalies"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    series_id: Mapped[str] = mapped_column(String(100), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    anomaly_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    anomaly_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[float] = mapped_column(Float, nullable=False)
    expected_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    actual_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    z_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    detection_method: Mapped[str] = mapped_column(String(50), nullable=False)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        Index("idx_anomalies_series", "series_id", "detected_at"),
        Index("idx_anomalies_timestamp", "anomaly_timestamp"),
    )


class Forecast(Base):
    """Model for forecasts."""

    __tablename__ = "forecasts"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    target_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_id: Mapped[str] = mapped_column(String(100), nullable=False)
    forecast_horizon_days: Mapped[int] = mapped_column(Integer, nullable=False)
    prediction: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    confidence_interval: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    model_used: Mapped[str | None] = mapped_column(String(100), nullable=True)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (Index("idx_forecasts_target", "target_type", "target_id", "created_at"),)


class CollectionJob(Base):
    """Model for tracking data collection jobs."""

    __tablename__ = "collection_jobs"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    collector_name: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="running")
    records_collected: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'completed', 'failed')", name="check_job_status"
        ),
        Index("idx_collection_jobs_collector", "collector_name", "started_at"),
    )


class SystemLog(Base):
    """Persistent storage for application log entries.

    Captures structlog output at WARNING level and above so users can
    review errors, data-quality issues, and operational events through
    the Reports tab in the GUI.
    """

    __tablename__ = "system_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    level: Mapped[str] = mapped_column(String(20), nullable=False)  # WARNING, ERROR, CRITICAL
    logger_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    event: Mapped[str] = mapped_column(Text, nullable=False)  # The log message / event name
    source_module: Mapped[str | None] = mapped_column(String(200), nullable=True)
    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "level IN ('WARNING', 'ERROR', 'CRITICAL', 'INFO')",
            name="check_system_log_level",
        ),
        Index("idx_system_logs_timestamp", "timestamp"),
        Index("idx_system_logs_level", "level", "timestamp"),
        Index("idx_system_logs_source", "source_module", "timestamp"),
    )



class ResearchPaper(Base):
    """Model for research papers from multiple sources."""

    __tablename__ = "research_papers"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    doi: Mapped[str | None] = mapped_column(String(100), nullable=True, unique=True)
    arxiv_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    abstract: Mapped[str | None] = mapped_column(Text, nullable=True)
    authors: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    institutions: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    published_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # arxiv, semantic_scholar, etc.
    topics: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)

    # Validation scores
    quick_score: Mapped[float] = mapped_column(Float, default=0.5)
    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    reference_count: Mapped[int] = mapped_column(Integer, default=0)

    # Contrarian classification
    research_type: Mapped[str] = mapped_column(
        String(20), default="consensus"
    )  # consensus, emerging, contrarian
    contrarian_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    contradicts_papers: Mapped[list[str]] = mapped_column(
        ARRAY(String), default=list
    )  # DOIs of contradicted papers

    # Metadata
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    pdf_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    code_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "research_type IN ('consensus', 'emerging', 'contrarian', 'low_quality_contrarian')",
            name="check_research_type",
        ),
        CheckConstraint(
            "quick_score >= 0 AND quick_score <= 1", name="check_quick_score_range"
        ),
        Index("idx_research_papers_doi", "doi"),
        Index("idx_research_papers_arxiv", "arxiv_id"),
        Index("idx_research_papers_published", "published_date"),
        Index("idx_research_papers_source", "source", "published_date"),
        Index("idx_research_papers_type", "research_type", "published_date"),
    )


class ResearchSignal(Base):
    """Aggregated research signals for bottleneck detection."""

    __tablename__ = "research_signals"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )  # Time bucket
    topic: Mapped[str] = mapped_column(String(50), nullable=False)  # energy, semiconductors, etc.

    # Volume metrics
    paper_count: Mapped[int] = mapped_column(Integer, default=0)
    new_paper_count: Mapped[int] = mapped_column(Integer, default=0)  # Papers in last 7 days
    citation_velocity: Mapped[float] = mapped_column(Float, default=0.0)  # Average citations/day

    # Quality metrics
    avg_quick_score: Mapped[float] = mapped_column(Float, default=0.5)
    top_institution_ratio: Mapped[float] = mapped_column(
        Float, default=0.0
    )  # % from top-tier institutions

    # Trend signals
    contrarian_count: Mapped[int] = mapped_column(Integer, default=0)  # Contrarian papers in period
    emerging_count: Mapped[int] = mapped_column(Integer, default=0)  # Emerging papers in period
    consensus_shift: Mapped[float] = mapped_column(Float, default=0.0)  # Detected consensus changes

    extra_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        Index("idx_research_signals_topic", "topic", "timestamp"),
        Index("idx_research_signals_timestamp", "timestamp"),
    )


class AuthorCache(Base):
    """Cached author credibility data."""

    __tablename__ = "author_cache"

    author_id: Mapped[str] = mapped_column(
        String(100), primary_key=True
    )  # Semantic Scholar ID or name hash
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    h_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_citations: Mapped[int] = mapped_column(Integer, default=0)
    paper_count: Mapped[int] = mapped_column(Integer, default=0)
    top_institution: Mapped[bool] = mapped_column(Boolean, default=False)
    institution_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    __table_args__ = (Index("idx_author_cache_name", "name"),)



class WARNNotice(Base):
    """Model for WARN Act layoff notices from state labor departments."""

    __tablename__ = "warn_notices"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Company Information
    company_name: Mapped[str] = mapped_column(String(500), nullable=False)
    company_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(String(300), nullable=True)
    state: Mapped[str] = mapped_column(String(2), nullable=False)  # State code (e.g., CA, TX)
    zip_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    county: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Layoff Details
    notice_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    layoff_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # Alias for effective_date in some states
    received_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # When state received the notice
    employees_affected: Mapped[int] = mapped_column(Integer, nullable=False)
    layoff_type: Mapped[str] = mapped_column(
        String(50), default="layoff"
    )  # layoff, closure, relocation

    # Industry Classification
    naics_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    naics_description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    sector_category: Mapped[str | None] = mapped_column(
        String(50), nullable=True
    )  # Our internal sector mapping

    # Status and Notes
    is_temporary: Mapped[bool] = mapped_column(Boolean, default=False)
    is_closure: Mapped[bool] = mapped_column(Boolean, default=False)
    union_affected: Mapped[str | None] = mapped_column(String(500), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Source tracking
    source_state: Mapped[str] = mapped_column(String(2), nullable=False)  # State that published notice
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    data_source: Mapped[str] = mapped_column(
        String(20), default="scraped", server_default="scraped"
    )
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    # Cross-validation
    validation_status: Mapped[str | None] = mapped_column(
        String(30), nullable=True, default=None
    )
    validation_details: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True, default=None
    )
    last_validated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )

    __table_args__ = (
        CheckConstraint("employees_affected >= 0", name="check_employees_positive"),
        CheckConstraint(
            "layoff_type IN ('layoff', 'closure', 'relocation', 'furlough', 'unknown')",
            name="check_layoff_type",
        ),
        # Unique constraint to prevent duplicate notices
        UniqueConstraint(
            "company_name", "state", "notice_date", "employees_affected",
            name="uq_warn_notice"
        ),
        Index("idx_warn_notices_state", "state", "notice_date"),
        Index("idx_warn_notices_date", "notice_date"),
        Index("idx_warn_notices_sector", "sector_category", "notice_date"),
        Index("idx_warn_notices_naics", "naics_code"),
        Index("idx_warn_notices_company", "company_name"),
        Index("idx_warn_notices_data_source", "data_source"),
        Index("idx_warn_notices_validation", "validation_status"),
    )


class ScraperHealth(Base):
    """Tracks per-state WARN scraper reliability."""

    __tablename__ = "scraper_health"

    state: Mapped[str] = mapped_column(String(2), primary_key=True)
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_failure_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    total_runs: Mapped[int] = mapped_column(Integer, default=0)
    total_successes: Mapped[int] = mapped_column(Integer, default=0)
    last_record_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), default="healthy", server_default="healthy"
    )



class SECFilingSignal(Base):
    """Supply chain signals extracted from SEC filings.

    Stores keyword matches, risk factor extractions, and other signals
    from 10-K, 10-Q, and 8-K filings.
    """

    __tablename__ = "sec_filing_signals"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Company Information
    cik: Mapped[str] = mapped_column(String(10), nullable=False)
    company_name: Mapped[str] = mapped_column(String(300), nullable=False)
    ticker: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Filing Information
    filing_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 10-K, 10-Q, 8-K
    filing_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    accession_number: Mapped[str] = mapped_column(String(30), nullable=False)

    # Signal Data
    signal_type: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # keyword_match, risk_factor, mda, event_filing
    section: Mapped[str | None] = mapped_column(
        String(50), nullable=True
    )  # risk_factors, mda, 8-K
    extracted_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    keyword_matches: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    total_keyword_count: Mapped[int] = mapped_column(Integer, default=0)
    signal_strength: Mapped[float] = mapped_column(Float, default=0.0)

    # Sentiment and Classification
    sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    sector: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sic_code: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Metadata
    file_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "filing_type IN ('10-K', '10-Q', '8-K', '20-F', '6-K', '40-F')",
            name="check_sec_filing_type",
        ),
        CheckConstraint(
            "signal_strength >= 0 AND signal_strength <= 1",
            name="check_signal_strength_range",
        ),
        UniqueConstraint(
            "cik", "accession_number", "signal_type", "section",
            name="uq_sec_filing_signal"
        ),
        Index("idx_sec_signals_cik", "cik", "filing_date"),
        Index("idx_sec_signals_ticker", "ticker", "filing_date"),
        Index("idx_sec_signals_filing_date", "filing_date"),
        Index("idx_sec_signals_type", "filing_type", "signal_type", "filing_date"),
        Index("idx_sec_signals_sector", "sector", "filing_date"),
    )



class CommodityInventory(Base):
    """Commodity inventory levels from various sources.

    Tracks inventory/stock levels for metals, petroleum, grains, etc.
    from sources like LME, COMEX, EIA, and NASS.
    """

    __tablename__ = "commodity_inventories"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Commodity Information
    commodity: Mapped[str] = mapped_column(String(50), nullable=False)  # copper, gold, crude_oil
    commodity_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # metal, petroleum, grain, etc.
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # LME, COMEX, EIA, NASS

    # Location (for warehouse-based inventories)
    location: Mapped[str | None] = mapped_column(String(100), nullable=True)
    location_region: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Inventory Data
    quantity: Mapped[Decimal] = mapped_column(Numeric(20, 4), nullable=False)
    unit: Mapped[str] = mapped_column(String(30), nullable=False)  # metric_tons, barrels, bushels
    stock_type: Mapped[str | None] = mapped_column(
        String(30), nullable=True
    )  # registered, eligible, total
    change_from_prior: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    change_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Temporal Data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data_delay_days: Mapped[int] = mapped_column(Integer, default=0)  # For delayed data sources
    reporting_period: Mapped[str | None] = mapped_column(String(20), nullable=True)  # weekly, monthly

    # Metadata
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "commodity_type IN ('metal', 'petroleum', 'grain', 'softs', 'energy', 'other')",
            name="check_commodity_type",
        ),
        Index("idx_commodity_inv_commodity", "commodity", "timestamp"),
        Index("idx_commodity_inv_source", "source", "commodity", "timestamp"),
        Index("idx_commodity_inv_timestamp", "timestamp"),
        Index("idx_commodity_inv_type", "commodity_type", "timestamp"),
    )



class MineralTradeFlow(Base):
    """Critical mineral trade flows from UN Comtrade.

    Tracks import/export flows for critical minerals by country pairs.
    """

    __tablename__ = "mineral_trade_flows"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Mineral Information
    mineral: Mapped[str] = mapped_column(String(50), nullable=False)  # lithium, cobalt, etc.
    hs_code: Mapped[str] = mapped_column(String(10), nullable=False)  # Harmonized System code
    hs_description: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Trade Flow
    reporter_country: Mapped[str] = mapped_column(String(100), nullable=False)
    reporter_iso3: Mapped[str | None] = mapped_column(String(3), nullable=True)
    partner_country: Mapped[str] = mapped_column(String(100), nullable=False)
    partner_iso3: Mapped[str | None] = mapped_column(String(3), nullable=True)
    flow_type: Mapped[str] = mapped_column(String(10), nullable=False)  # import, export

    # Value and Quantity
    value_usd: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    quantity_unit: Mapped[str | None] = mapped_column(String(30), nullable=True)
    weight_kg: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)

    # Temporal Data
    period: Mapped[str] = mapped_column(String(6), nullable=False)  # YYYYMM format
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Metadata
    source: Mapped[str] = mapped_column(String(30), default="un_comtrade")
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "flow_type IN ('import', 'export', 're-import', 're-export')",
            name="check_flow_type",
        ),
        UniqueConstraint(
            "mineral", "hs_code", "reporter_iso3", "partner_iso3", "flow_type", "period",
            name="uq_mineral_trade_flow"
        ),
        Index("idx_mineral_flows_mineral", "mineral", "period"),
        Index("idx_mineral_flows_reporter", "reporter_iso3", "period"),
        Index("idx_mineral_flows_partner", "partner_iso3", "period"),
        Index("idx_mineral_flows_period", "period"),
        Index("idx_mineral_flows_hs", "hs_code", "period"),
    )


class MineralProduction(Base):
    """Critical mineral production data from USGS.

    Annual production, imports, exports, and consumption by country.
    """

    __tablename__ = "mineral_production"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Mineral Information
    mineral: Mapped[str] = mapped_column(String(50), nullable=False)
    usgs_commodity_code: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Country/Region
    country: Mapped[str] = mapped_column(String(100), nullable=False)
    country_iso3: Mapped[str | None] = mapped_column(String(3), nullable=True)

    # Production Data
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    production: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    production_unit: Mapped[str | None] = mapped_column(String(30), nullable=True)
    reserves: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    reserves_unit: Mapped[str | None] = mapped_column(String(30), nullable=True)

    # For US data
    us_imports: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    us_exports: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    us_apparent_consumption: Mapped[Decimal | None] = mapped_column(Numeric(20, 4), nullable=True)
    us_import_reliance_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Metadata
    source: Mapped[str] = mapped_column(String(30), default="usgs_mcs")
    data_vintage: Mapped[str | None] = mapped_column(String(10), nullable=True)  # e.g., "2025"
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "mineral", "country", "year", "source",
            name="uq_mineral_production"
        ),
        Index("idx_mineral_prod_mineral", "mineral", "year"),
        Index("idx_mineral_prod_country", "country", "year"),
        Index("idx_mineral_prod_year", "year"),
    )



class ShippingIndex(Base):
    """Shipping and freight rate indices.

    Tracks major shipping indices including:
    - Baltic Dry Index (BDI) - Dry bulk shipping rates
    - Baltic indices by vessel type (Capesize, Panamax, Supramax, Handysize)
    - Freightos Baltic Index (FBX) - Container shipping rates
    - Drewry World Container Index
    - SCFI (Shanghai Containerized Freight Index)
    """

    __tablename__ = "shipping_indices"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Index Information
    index_name: Mapped[str] = mapped_column(String(50), nullable=False)  # BDI, FBX, SCFI
    index_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # bulk, container, tanker, overall
    sub_index: Mapped[str | None] = mapped_column(
        String(50), nullable=True
    )  # Capesize, Panamax, transpacific, etc.

    # Index Values
    value: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    unit: Mapped[str] = mapped_column(String(30), default="points")  # points, USD/TEU, USD/day
    change_from_prior: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    change_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Time-weighted averages (for comparison)
    week_average: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    month_average: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    year_ago_value: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)

    # Temporal Data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reporting_period: Mapped[str] = mapped_column(String(20), default="daily")  # daily, weekly

    # Metadata
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # baltic_exchange, freightos
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "index_type IN ('bulk', 'container', 'tanker', 'overall', 'lng', 'lpg')",
            name="check_shipping_index_type",
        ),
        UniqueConstraint(
            "index_name", "sub_index", "timestamp",
            name="uq_shipping_index"
        ),
        Index("idx_shipping_idx_name", "index_name", "timestamp"),
        Index("idx_shipping_idx_type", "index_type", "timestamp"),
        Index("idx_shipping_idx_timestamp", "timestamp"),
        Index("idx_shipping_idx_source", "source", "timestamp"),
    )


class PortThroughput(Base):
    """Port container throughput and cargo volume data.

    Tracks TEU (Twenty-foot Equivalent Unit) volumes and other cargo
    metrics by port. Key ports include:
    - Port of Los Angeles / Long Beach (US West Coast)
    - Port of New York/New Jersey (US East Coast)
    - Port of Savannah, Houston, Seattle/Tacoma
    - Major global ports (Shanghai, Singapore, Rotterdam)
    """

    __tablename__ = "port_throughput"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Port Information
    port_name: Mapped[str] = mapped_column(String(100), nullable=False)
    port_code: Mapped[str | None] = mapped_column(String(10), nullable=True)  # UN/LOCODE
    country: Mapped[str] = mapped_column(String(100), default="United States")
    region: Mapped[str | None] = mapped_column(
        String(50), nullable=True
    )  # West Coast, East Coast, Gulf

    # Throughput Data
    metric_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # teu_total, teu_imports, teu_exports, teu_empties
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), default="TEU")  # TEU, metric_tons, containers

    # Comparisons
    change_from_prior_period: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)
    change_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    year_over_year_change_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Temporal Data
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reporting_period: Mapped[str] = mapped_column(String(20), default="monthly")  # monthly, weekly
    period_label: Mapped[str | None] = mapped_column(String(20), nullable=True)  # "2025-01", "Week 4"

    # Metadata
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # port_la, port_lb, port_ny
    is_preliminary: Mapped[bool] = mapped_column(Boolean, default=False)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "metric_type IN ('teu_total', 'teu_imports', 'teu_exports', 'teu_empties', "
            "'teu_loaded', 'cargo_tons', 'vessel_calls', 'rail_units')",
            name="check_port_metric_type",
        ),
        UniqueConstraint(
            "port_name", "metric_type", "period_start",
            name="uq_port_throughput"
        ),
        Index("idx_port_throughput_port", "port_name", "period_start"),
        Index("idx_port_throughput_period", "period_start"),
        Index("idx_port_throughput_metric", "metric_type", "period_start"),
        Index("idx_port_throughput_region", "region", "period_start"),
        Index("idx_port_throughput_source", "source", "period_start"),
    )


class ContainerDwellTime(Base):
    """Container dwell times at ports and terminals.

    Tracks how long containers stay at ports, terminals, and rail yards.
    Elevated dwell times indicate congestion and supply chain stress.
    """

    __tablename__ = "container_dwell_times"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Location Information
    port_name: Mapped[str] = mapped_column(String(100), nullable=False)
    terminal_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    location_type: Mapped[str] = mapped_column(
        String(30), default="port"
    )  # port, rail_yard, warehouse, chassis_depot

    # Dwell Time Metrics
    container_type: Mapped[str] = mapped_column(
        String(30), default="all"
    )  # all, import, export, empty, reefer
    dwell_time_avg_days: Mapped[float] = mapped_column(Float, nullable=False)
    dwell_time_median_days: Mapped[float | None] = mapped_column(Float, nullable=True)
    dwell_time_p90_days: Mapped[float | None] = mapped_column(Float, nullable=True)  # 90th percentile
    containers_measured: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Threshold Analysis
    pct_over_5_days: Mapped[float | None] = mapped_column(Float, nullable=True)  # % over 5-day threshold
    pct_over_9_days: Mapped[float | None] = mapped_column(Float, nullable=True)  # % over 9-day threshold
    target_days: Mapped[float | None] = mapped_column(Float, nullable=True)  # Target dwell time

    # Comparisons
    change_from_prior: Mapped[float | None] = mapped_column(Float, nullable=True)
    year_ago_dwell: Mapped[float | None] = mapped_column(Float, nullable=True)
    historical_avg_dwell: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Temporal Data
    measurement_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reporting_period: Mapped[str] = mapped_column(String(20), default="weekly")

    # Metadata
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # port authority, PMSA, etc.
    methodology_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "location_type IN ('port', 'terminal', 'rail_yard', 'warehouse', 'chassis_depot')",
            name="check_dwell_location_type",
        ),
        CheckConstraint(
            "container_type IN ('all', 'import', 'export', 'empty', 'reefer', 'loaded')",
            name="check_container_type",
        ),
        CheckConstraint(
            "dwell_time_avg_days >= 0",
            name="check_dwell_time_positive",
        ),
        UniqueConstraint(
            "port_name", "terminal_name", "container_type", "measurement_date",
            name="uq_container_dwell"
        ),
        Index("idx_dwell_port", "port_name", "measurement_date"),
        Index("idx_dwell_date", "measurement_date"),
        Index("idx_dwell_type", "container_type", "measurement_date"),
        Index("idx_dwell_location", "location_type", "measurement_date"),
    )



class IOCoefficient(Base):
    """Model for BEA Input-Output coefficients.

    Stores inter-industry dependency data from BEA's Input-Output tables.
    Coefficients represent the value of inputs from one industry required
    to produce one dollar of output in another industry.

    Table Types:
    - direct_requirements: Immediate inputs needed
    - total_requirements: Direct + indirect inputs (Leontief inverse)
    - make: What each industry produces (commodities)
    - use: What inputs each industry consumes
    - supply: Total domestic supply by commodity
    - import_matrix: Imported commodities by industry
    """

    __tablename__ = "io_coefficients"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    table_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # See check constraint for valid values
    detail_level: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # 'sector' (15), 'summary' (71), 'detail' (402)
    from_industry: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # BEA industry code (source)
    from_industry_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    to_industry: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # BEA industry code (consumer)
    to_industry_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    coefficient: Mapped[Decimal] = mapped_column(
        Numeric(12, 10), nullable=False
    )  # The I-O coefficient
    # Commodity fields for Make/Use/Supply tables
    commodity_code: Mapped[str | None] = mapped_column(
        String(20), nullable=True
    )  # BEA commodity code (for Make/Use/Supply tables)
    commodity_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        CheckConstraint(
            "table_type IN ('direct_requirements', 'total_requirements', "
            "'make', 'use', 'supply', 'import_matrix')",
            name="check_io_table_type",
        ),
        CheckConstraint(
            "detail_level IN ('sector', 'summary', 'detail')",
            name="check_io_detail_level",
        ),
        UniqueConstraint(
            "year", "table_type", "detail_level", "from_industry", "to_industry",
            name="uq_io_coefficient"
        ),
        Index("idx_io_coef_year_type", "year", "table_type"),
        Index("idx_io_coef_from", "from_industry"),
        Index("idx_io_coef_to", "to_industry"),
        Index("idx_io_coef_detail", "detail_level", "year"),
        Index("idx_io_coef_commodity", "commodity_code"),
    )



class VehicleProduction(Base):
    """Vehicle production, shipments, and inventory data.

    Tracks motor vehicle manufacturing activity from multiple sources:
    - Census M3: Manufacturer shipments and inventories (monthly)
    - FRED: Auto Inventory/Sales Ratio (AISRSA)
    - Cox Automotive: Days supply and used vehicle metrics
    - BEA: Motor vehicle output (quarterly GDP component)
    """

    __tablename__ = "vehicle_production"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Metric Classification
    metric_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # shipments, inventory, output, inventory_ratio, days_supply
    vehicle_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # light_vehicle, truck, auto, total, used

    # Value Data
    value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    unit: Mapped[str] = mapped_column(String(30), nullable=False)  # million_dollars, ratio, days, units
    change_from_prior: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    change_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Seasonal Adjustment
    seasonal_adjustment: Mapped[str | None] = mapped_column(
        String(10), nullable=True
    )  # SA, NSA, SAAR

    # Temporal Data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reporting_period: Mapped[str] = mapped_column(String(20), default="monthly")  # monthly, quarterly

    # Metadata
    source: Mapped[str] = mapped_column(String(30), nullable=False)  # CENSUS_M3, FRED, COX, BEA
    series_id: Mapped[str | None] = mapped_column(String(50), nullable=True)  # Original series identifier
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "metric_type IN ('shipments', 'inventory', 'output', 'inventory_ratio', "
            "'days_supply', 'production', 'sales', 'orders')",
            name="check_vehicle_metric_type",
        ),
        CheckConstraint(
            "vehicle_type IN ('light_vehicle', 'truck', 'auto', 'total', 'used', "
            "'commercial', 'ev', 'hybrid', 'parts')",
            name="check_vehicle_type",
        ),
        UniqueConstraint(
            "source", "metric_type", "vehicle_type", "timestamp", "seasonal_adjustment",
            name="uq_vehicle_production"
        ),
        Index("idx_vehicle_prod_metric", "metric_type", "timestamp"),
        Index("idx_vehicle_prod_type", "vehicle_type", "timestamp"),
        Index("idx_vehicle_prod_source", "source", "timestamp"),
        Index("idx_vehicle_prod_timestamp", "timestamp"),
    )


class ShipbuildingOrder(Base):
    """Shipbuilding orders, deliveries, backlog, and fleet data.

    Tracks shipbuilding activity from:
    - MARAD: US-flag fleet statistics and shipyard data
    - UNCTAD: Global fleet and orderbook statistics
    """

    __tablename__ = "shipbuilding_orders"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Shipyard Information
    shipyard: Mapped[str | None] = mapped_column(String(150), nullable=True)
    country: Mapped[str | None] = mapped_column(String(100), nullable=True)  # Shipyard country

    # Vessel Classification
    vessel_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # container, tanker, bulk, lng, cruise, naval, other

    # Metric Data
    metric_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # orders, deliveries, backlog, capacity, fleet
    value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)  # vessels, dwt, gt, teu, cgt
    change_from_prior: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    change_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Order Details (for individual orders)
    vessel_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    owner: Mapped[str | None] = mapped_column(String(150), nullable=True)
    expected_delivery: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Temporal Data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reporting_period: Mapped[str] = mapped_column(String(20), default="monthly")

    # Metadata
    source: Mapped[str] = mapped_column(String(30), nullable=False)  # MARAD, UNCTAD, CLARKSONS
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "vessel_type IN ('container', 'tanker', 'bulk', 'lng', 'lpg', 'cruise', "
            "'roro', 'naval', 'offshore', 'other', 'total')",
            name="check_vessel_type",
        ),
        CheckConstraint(
            "metric_type IN ('orders', 'deliveries', 'backlog', 'capacity', 'fleet', "
            "'cancellations', 'orderbook', 'slippage')",
            name="check_shipbuilding_metric_type",
        ),
        Index("idx_shipbuilding_vessel", "vessel_type", "timestamp"),
        Index("idx_shipbuilding_metric", "metric_type", "timestamp"),
        Index("idx_shipbuilding_source", "source", "timestamp"),
        Index("idx_shipbuilding_timestamp", "timestamp"),
        Index("idx_shipbuilding_country", "country", "timestamp"),
    )


class AircraftProduction(Base):
    """Aircraft, aerospace, and drone production/trade data.

    Tracks aerospace manufacturing activity from:
    - Census HS Trade: Chapter 88 aircraft/parts imports and exports
    - FAA Registry: Aircraft registration statistics
    - USASpending: DOD aerospace contract awards

    This table consolidates multiple aerospace data types:
    - Trade flows (imports/exports by HS code)
    - Aircraft registrations (by type, manufacturer)
    - DOD contract awards (by PSC/NAICS code)
    """

    __tablename__ = "aircraft_production"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Record Type
    record_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # trade, registration, contract

    # Classification
    aircraft_category: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # powered_aircraft, parts, drones, spacecraft, ground_equipment
    sub_category: Mapped[str | None] = mapped_column(
        String(50), nullable=True
    )  # helicopter, fixed_wing, uas, engine, avionics

    # Trade Data (for record_type='trade')
    hs_code: Mapped[str | None] = mapped_column(String(10), nullable=True)  # HS Chapter 88 codes
    flow_type: Mapped[str | None] = mapped_column(String(10), nullable=True)  # import, export
    partner_country: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Contract Data (for record_type='contract')
    award_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    recipient: Mapped[str | None] = mapped_column(String(200), nullable=True)
    psc_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    naics_code: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Registration Data (for record_type='registration')
    manufacturer: Mapped[str | None] = mapped_column(String(100), nullable=True)
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    owner_type: Mapped[str | None] = mapped_column(String(30), nullable=True)

    # Value Data
    value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    unit: Mapped[str] = mapped_column(String(30), nullable=False)  # USD, units, count
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), nullable=True)
    quantity_unit: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Drone/UAS Flags
    is_drone_related: Mapped[bool] = mapped_column(Boolean, default=False)
    is_military: Mapped[bool] = mapped_column(Boolean, default=False)

    # Temporal Data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reporting_period: Mapped[str] = mapped_column(String(20), default="monthly")

    # Metadata
    source: Mapped[str] = mapped_column(String(30), nullable=False)  # CENSUS_HS, FAA, USASPENDING
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "record_type IN ('trade', 'registration', 'contract', 'production')",
            name="check_aircraft_record_type",
        ),
        CheckConstraint(
            "aircraft_category IN ('powered_aircraft', 'parts', 'drones', 'spacecraft', "
            "'ground_equipment', 'lighter_than_air', 'parachutes', 'engines', 'avionics')",
            name="check_aircraft_category",
        ),
        Index("idx_aircraft_category", "aircraft_category", "timestamp"),
        Index("idx_aircraft_record_type", "record_type", "timestamp"),
        Index("idx_aircraft_source", "source", "timestamp"),
        Index("idx_aircraft_timestamp", "timestamp"),
        Index("idx_aircraft_hs", "hs_code", "timestamp"),
        Index("idx_aircraft_drone", "is_drone_related", "timestamp"),
        Index("idx_aircraft_manufacturer", "manufacturer"),
    )


class AlertHistory(Base):
    """Model for alert history with prioritization and deduplication tracking.

    Stores processed alerts with their priority, deduplication status,
    and acknowledgment state for historical analysis and audit trails.
    """

    __tablename__ = "alert_history"

    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Alert Classification
    alert_type: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # bottleneck, price_spike, inventory, labor, etc.
    category: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # BottleneckCategory value or custom
    subcategory: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Severity and Priority
    severity: Mapped[float] = mapped_column(Float, nullable=False)  # 0-1 scale
    priority: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # critical, high, medium, low

    # Deduplication
    dedup_key: Mapped[str] = mapped_column(String(32), nullable=False)
    is_duplicate: Mapped[bool] = mapped_column(Boolean, default=False)

    # Content
    description: Mapped[str] = mapped_column(Text, nullable=False)
    affected_sectors: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    source_series: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # User who acknowledged (for future user system integration)
    acknowledged_by: Mapped[str | None] = mapped_column(String(100), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "priority IN ('critical', 'high', 'medium', 'low')",
            name="check_alert_priority",
        ),
        CheckConstraint(
            "severity >= 0 AND severity <= 1",
            name="check_alert_severity_range",
        ),
        Index("idx_alert_history_created", "created_at"),
        Index("idx_alert_history_priority", "priority", "created_at"),
        Index("idx_alert_history_category", "category", "created_at"),
        Index("idx_alert_history_type", "alert_type", "created_at"),
        Index("idx_alert_history_dedup", "dedup_key", "created_at"),
        Index("idx_alert_history_unack", "acknowledged_at", "created_at"),
    )
