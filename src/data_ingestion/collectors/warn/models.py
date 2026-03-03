"""Data models for the WARN collector."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class WARNRecord:
    """Parsed WARN notice record."""

    company_name: str
    state: str
    notice_date: datetime
    employees_affected: int
    effective_date: datetime | None = None
    company_address: str | None = None
    city: str | None = None
    zip_code: str | None = None
    county: str | None = None
    naics_code: str | None = None
    naics_description: str | None = None
    sector_category: str | None = None
    layoff_type: str = "layoff"
    is_temporary: bool = False
    is_closure: bool = False
    union_affected: str | None = None
    reason: str | None = None
    notes: str | None = None
    source_url: str | None = None
    data_source: str = "scraped"
    raw_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class StateWARNConfig:
    """Configuration for a state's WARN data source."""

    state_code: str
    name: str
    url: str
    format: str  # "excel", "html", "csv", "pdf", "js_fallback"
    parser: str  # Parser function name
    schedule_tier: str = "weekly"  # "daily", "twice_weekly", "weekly"
    enabled: bool = True
    notes: str | None = None
