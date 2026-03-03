"""Shared parsing utilities for WARN collectors."""

import re
from datetime import datetime
from typing import Any

import pandas as pd

from src.config.sectors import get_sector_for_naics


def normalize_naics(naics_raw: str | None) -> tuple[str | None, str | None]:
    """Extract numeric NAICS code and map to sector category."""
    if not naics_raw:
        return None, None
    naics_match = re.search(r"(\d{2,6})", str(naics_raw))
    if not naics_match:
        return None, None
    naics_code = naics_match.group(1)
    sector = get_sector_for_naics(naics_code)
    sector_category = sector.value if sector else None
    return naics_code, sector_category


def parse_date(date_str: str | None, formats: list[str] | None = None) -> datetime | None:
    """Parse a date string into a datetime, trying multiple formats."""
    if not date_str or pd.isna(date_str):
        return None
    date_str = str(date_str).strip()
    if not date_str:
        return None

    default_formats = [
        "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y",
        "%B %d, %Y", "%b %d, %Y", "%d-%b-%Y", "%Y/%m/%d",
        "%m/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in (formats or default_formats):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    try:
        return pd.to_datetime(date_str).to_pydatetime()
    except (ValueError, TypeError):
        return None


def parse_employees(emp_str: str | int | None) -> int:
    """Parse employee count from various formats."""
    if emp_str is None or (isinstance(emp_str, float) and pd.isna(emp_str)):
        return 0
    if isinstance(emp_str, int):
        return emp_str
    emp_str = str(emp_str).strip()
    emp_str = re.sub(r"[,\s]", "", emp_str)
    range_match = re.match(r"(\d+)\s*[-–]\s*(\d+)", emp_str)
    if range_match:
        return int(range_match.group(2))
    num_match = re.search(r"(\d+)", emp_str)
    if num_match:
        return int(num_match.group(1))
    return 0


def detect_layoff_type(
    record: dict[str, Any],
    text_fields: list[str] | None = None,
) -> tuple[str, bool, bool]:
    """Detect layoff type from record fields. Returns (type, is_temporary, is_closure)."""
    text_fields = text_fields or ["reason", "notes", "type", "action"]
    combined_text = ""
    for field_name in text_fields:
        if field_name in record and record[field_name]:
            combined_text += " " + str(record[field_name]).lower()

    is_closure = any(
        kw in combined_text
        for kw in ["closure", "closing", "shut down", "shutdown", "permanent closure"]
    )
    is_temporary = any(
        kw in combined_text
        for kw in ["temporary", "furlough", "reduced hours", "seasonal"]
    )
    if is_closure:
        layoff_type = "closure"
    elif "relocation" in combined_text or "relocating" in combined_text:
        layoff_type = "relocation"
    elif is_temporary:
        layoff_type = "furlough"
    else:
        layoff_type = "layoff"
    return layoff_type, is_temporary, is_closure


def trunc(value: str | None, max_len: int) -> str | None:
    """Truncate a string to max_len, preserving None."""
    if value is None:
        return None
    return value[:max_len] if len(value) > max_len else value
