"""CSV-based WARN parsers."""

import csv
import io

import structlog

from src.data_ingestion.collectors.warn.models import StateWARNConfig, WARNRecord
from src.data_ingestion.collectors.warn.parse_utils import (
    normalize_naics,
    parse_date,
    parse_employees,
)

logger = structlog.get_logger()


async def parse_oregon_csv(content: bytes, config: StateWARNConfig) -> list[WARNRecord]:
    """Parse Oregon WARN data from their Socrata open-data CSV endpoint."""
    records: list[WARNRecord] = []
    try:
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            try:
                company = (row.get("Company") or row.get("Employer") or "").strip()
                if not company:
                    continue
                notice_date = parse_date(
                    row.get("Notice_Date") or row.get("Receive_Date") or row.get("Date")
                )
                if not notice_date:
                    continue
                employees = parse_employees(
                    row.get("Number_Affected") or row.get("Employees") or row.get("NumberAffected")
                )
                naics_code, sector = normalize_naics(row.get("NAICS") or row.get("Industry"))
                records.append(WARNRecord(
                    company_name=company,
                    state="OR",
                    notice_date=notice_date,
                    employees_affected=employees,
                    effective_date=parse_date(row.get("Effective_Date") or row.get("LayoffDate")),
                    city=row.get("City") or row.get("Location") or None,
                    county=row.get("County") or None,
                    naics_code=naics_code,
                    sector_category=sector,
                    source_url=config.url,
                    raw_data=dict(row),
                ))
            except Exception as e:
                logger.warning("Failed to parse OR CSV row", error=str(e))
    except Exception as e:
        logger.error("Failed to parse Oregon WARN CSV", error=str(e))
    return records
