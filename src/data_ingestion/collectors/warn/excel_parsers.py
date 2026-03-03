"""Excel-based WARN parsers for states that publish .xlsx files."""

import io
from typing import Any

import pandas as pd
import structlog

from src.data_ingestion.collectors.warn.models import StateWARNConfig, WARNRecord
from src.data_ingestion.collectors.warn.parse_utils import (
    detect_layoff_type,
    normalize_naics,
    parse_date,
    parse_employees,
)

logger = structlog.get_logger()


async def parse_california(content: bytes, config: StateWARNConfig) -> list[WARNRecord]:
    """Parse California WARN notices from Excel file."""
    records: list[WARNRecord] = []
    try:
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        for _, row in df.iterrows():
            try:
                notice_date = parse_date(row.get("Notice Date"))
                if not notice_date:
                    continue
                employees = parse_employees(row.get("No. Of Employees", 0))
                if employees == 0:
                    employees = parse_employees(row.get("Employees", 0))
                naics_code, sector_category = normalize_naics(
                    row.get("NAICS") or row.get("Industry")
                )
                layoff_type, is_temporary, is_closure = detect_layoff_type(
                    row.to_dict(), ["Layoff/Closure", "Type"]
                )
                records.append(WARNRecord(
                    company_name=str(row.get("Company Name", "Unknown")).strip(),
                    state="CA",
                    notice_date=notice_date,
                    employees_affected=employees,
                    effective_date=parse_date(row.get("Effective Date")),
                    city=str(row.get("City", "")).strip() or None,
                    county=str(row.get("County", "")).strip() or None,
                    naics_code=naics_code,
                    sector_category=sector_category,
                    layoff_type=layoff_type,
                    is_temporary=is_temporary,
                    is_closure=is_closure,
                    source_url=config.url,
                    raw_data=_safe_row_dict(row),
                ))
            except Exception as e:
                logger.warning("Failed to parse CA row", error=str(e))
    except Exception as e:
        logger.error("Failed to parse California WARN file", error=str(e))
    return records


async def parse_texas(content: bytes, config: StateWARNConfig) -> list[WARNRecord]:
    """Parse Texas WARN notices from Excel file."""
    records: list[WARNRecord] = []
    try:
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        for _, row in df.iterrows():
            try:
                notice_date = parse_date(row.get("Notice Date"))
                if not notice_date:
                    continue
                employees = parse_employees(
                    row.get("# Affected Workers", row.get("Employees", 0))
                )
                naics_code, sector_category = normalize_naics(row.get("NAICS"))
                layoff_type, is_temporary, is_closure = detect_layoff_type(
                    row.to_dict(), ["Type of Event", "Action"]
                )
                records.append(WARNRecord(
                    company_name=str(row.get("Company", "Unknown")).strip(),
                    state="TX",
                    notice_date=notice_date,
                    employees_affected=employees,
                    effective_date=parse_date(row.get("Layoff Date")),
                    city=str(row.get("City", "")).strip() or None,
                    county=str(row.get("County", "")).strip() or None,
                    naics_code=naics_code,
                    sector_category=sector_category,
                    layoff_type=layoff_type,
                    is_temporary=is_temporary,
                    is_closure=is_closure,
                    source_url=config.url,
                    raw_data=_safe_row_dict(row),
                ))
            except Exception as e:
                logger.warning("Failed to parse TX row", error=str(e))
    except Exception as e:
        logger.error("Failed to parse Texas WARN file", error=str(e))
    return records


def _safe_row_dict(row: Any) -> dict[str, Any]:
    """Convert a pandas row to a JSON-safe dictionary."""
    d = row.to_dict()
    result: dict[str, Any] = {}
    for k, v in d.items():
        if pd.isna(v):
            result[str(k)] = None
        elif hasattr(v, "isoformat"):
            result[str(k)] = v.isoformat()
        else:
            result[str(k)] = v
    return result
