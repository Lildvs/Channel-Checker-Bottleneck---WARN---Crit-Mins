"""PDF-based WARN parsers for states that publish notices as PDF documents."""

import io
import re

import structlog

from src.data_ingestion.collectors.warn.models import StateWARNConfig, WARNRecord
from src.data_ingestion.collectors.warn.parse_utils import (
    normalize_naics,
    parse_date,
    parse_employees,
)

logger = structlog.get_logger()

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    logger.warning("pdfplumber not installed -- PDF WARN parsing unavailable")


def _extract_tables_from_pdf(content: bytes) -> list[list[list[str]]]:
    """Extract all tables from all pages of a PDF."""
    if not HAS_PDFPLUMBER:
        return []
    tables: list[list[list[str]]] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            if page_tables:
                tables.extend(page_tables)
    return tables


def _find_header_indices(
    headers: list[str],
    company_patterns: list[str],
    date_patterns: list[str],
    employee_patterns: list[str],
) -> tuple[int, int, int]:
    """Find column indices for company, date, and employee count."""
    company_idx = date_idx = emp_idx = -1
    for i, h in enumerate(headers):
        hl = h.lower().strip()
        if company_idx < 0 and any(p in hl for p in company_patterns):
            company_idx = i
        elif date_idx < 0 and any(p in hl for p in date_patterns):
            date_idx = i
        elif emp_idx < 0 and any(p in hl for p in employee_patterns):
            emp_idx = i
    return company_idx, date_idx, emp_idx


def _parse_pdf_table(
    table: list[list[str]],
    config: StateWARNConfig,
) -> list[WARNRecord]:
    """Parse a single extracted PDF table into WARN records."""
    if not table or len(table) < 2:
        return []

    headers = [str(cell or "").strip() for cell in table[0]]
    company_idx, date_idx, emp_idx = _find_header_indices(
        headers,
        ["company", "employer", "business", "name", "organization"],
        ["notice", "date", "received", "warn"],
        ["employee", "worker", "affected", "number", "#"],
    )
    if company_idx < 0 or date_idx < 0:
        return []

    records: list[WARNRecord] = []
    for row in table[1:]:
        try:
            if len(row) <= max(company_idx, date_idx):
                continue
            company = str(row[company_idx] or "").strip()
            if not company:
                continue
            notice_date = parse_date(str(row[date_idx] or "").strip())
            if not notice_date:
                continue
            employees = 0
            if emp_idx >= 0 and emp_idx < len(row):
                employees = parse_employees(str(row[emp_idx] or "").strip())

            raw = {headers[i]: str(row[i] or "").strip() for i in range(min(len(headers), len(row)))}
            naics_code, sector = normalize_naics(raw.get("naics") or raw.get("industry"))

            records.append(WARNRecord(
                company_name=company,
                state=config.state_code,
                notice_date=notice_date,
                employees_affected=employees,
                city=raw.get("city") or raw.get("location") or None,
                county=raw.get("county") or None,
                naics_code=naics_code,
                sector_category=sector,
                source_url=config.url,
                raw_data=raw,
            ))
        except Exception as e:
            logger.warning("Failed to parse PDF row", state=config.state_code, error=str(e))
    return records


async def parse_idaho_pdf(content: bytes, config: StateWARNConfig) -> list[WARNRecord]:
    """Parse Idaho WARN PDF notices."""
    if not HAS_PDFPLUMBER:
        logger.error("Cannot parse Idaho PDF -- pdfplumber not installed")
        return []
    records: list[WARNRecord] = []
    try:
        tables = _extract_tables_from_pdf(content)
        for table in tables:
            records.extend(_parse_pdf_table(table, config))
    except Exception as e:
        logger.error("Failed to parse Idaho WARN PDF", error=str(e))
    return records


async def parse_newmexico_pdf(content: bytes, config: StateWARNConfig) -> list[WARNRecord]:
    """Parse New Mexico WARN PDF notices."""
    if not HAS_PDFPLUMBER:
        logger.error("Cannot parse NM PDF -- pdfplumber not installed")
        return []
    records: list[WARNRecord] = []
    try:
        tables = _extract_tables_from_pdf(content)
        for table in tables:
            records.extend(_parse_pdf_table(table, config))
    except Exception as e:
        logger.error("Failed to parse NM WARN PDF", error=str(e))
    return records
