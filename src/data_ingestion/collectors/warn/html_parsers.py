"""HTML-based WARN parsers -- generic parser + state-specific overrides."""

import re

import structlog
from bs4 import BeautifulSoup, Tag

from src.data_ingestion.collectors.warn.models import StateWARNConfig, WARNRecord
from src.data_ingestion.collectors.warn.parse_utils import (
    normalize_naics,
    parse_date,
    parse_employees,
)

logger = structlog.get_logger()

COMPANY_HEADERS = frozenset({
    "company", "employer", "company name", "employer name", "business",
    "organization", "firm", "name", "company/employer",
})

DATE_HEADERS = frozenset({
    "notice date", "date", "warn date", "notice", "event date",
    "layoff date", "closing date", "date received", "received date",
    "date of notice", "warn notice date",
})

EMPLOYEE_HEADERS = frozenset({
    "employees", "# affected", "number affected", "workers",
    "# employees", "number of employees", "total employees",
    "affected employees", "headcount", "job losses",
    "no. of employees", "employees affected", "# of employees",
    "number of workers", "affected workers",
})


def _find_best_table(soup: BeautifulSoup) -> Tag | None:
    """Find the table most likely to contain WARN data.

    Strategy: score each table by how many of our known header patterns
    it matches, then pick the highest scorer with at least 1 data row.
    """
    best_table = None
    best_score = 0

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [
            cell.get_text(strip=True).lower()
            for cell in header_row.find_all(["th", "td"])
        ]
        if not headers:
            continue

        score = 0
        for h in headers:
            if any(pat in h for pat in COMPANY_HEADERS):
                score += 3
            if any(pat in h for pat in DATE_HEADERS):
                score += 2
            if any(pat in h for pat in EMPLOYEE_HEADERS):
                score += 2

        data_rows = len(table.find_all("tr")) - 1
        if score > best_score and data_rows > 0:
            best_score = score
            best_table = table

    return best_table


def _extract_field(row_data: dict[str, str], candidates: frozenset[str]) -> str | None:
    """Find the first matching field from row data."""
    for key in candidates:
        if key in row_data and row_data[key]:
            return row_data[key]
    return None


async def parse_generic_html(
    content: bytes, config: StateWARNConfig
) -> list[WARNRecord]:
    """Improved generic HTML table parser with expanded header matching."""
    records: list[WARNRecord] = []
    try:
        soup = BeautifulSoup(content, "lxml")
        table = _find_best_table(soup)

        if table is None:
            for t in soup.find_all("table"):
                header_row = t.find("tr")
                if header_row:
                    headers_text = " ".join(
                        cell.get_text(strip=True).lower()
                        for cell in header_row.find_all(["th", "td"])
                    )
                    if any(pat in headers_text for pat in COMPANY_HEADERS):
                        table = t
                        break

        if table is None:
            logger.info("No WARN table found", state=config.state_code)
            return records

        header_row = table.find("tr")
        headers: list[str] = []
        if header_row:
            headers = [
                cell.get_text(strip=True).lower()
                for cell in header_row.find_all(["th", "td"])
            ]

        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            try:
                row_data: dict[str, str] = {}
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        row_data[headers[i]] = cell.get_text(strip=True)

                company_name = _extract_field(row_data, COMPANY_HEADERS)
                if not company_name:
                    continue

                date_str = _extract_field(row_data, DATE_HEADERS)
                notice_date = parse_date(date_str) if date_str else None
                if not notice_date:
                    continue

                emp_str = _extract_field(row_data, EMPLOYEE_HEADERS)
                employees = parse_employees(emp_str) if emp_str else 0

                naics_code, sector_category = normalize_naics(
                    row_data.get("naics") or row_data.get("industry")
                )

                records.append(WARNRecord(
                    company_name=company_name,
                    state=config.state_code,
                    notice_date=notice_date,
                    employees_affected=employees,
                    effective_date=parse_date(
                        row_data.get("effective date")
                        or row_data.get("layoff date")
                    ),
                    city=row_data.get("city") or row_data.get("location") or None,
                    county=row_data.get("county") or None,
                    naics_code=naics_code,
                    sector_category=sector_category,
                    source_url=config.url,
                    raw_data=row_data,
                ))
            except Exception as e:
                logger.warning("Failed to parse row", state=config.state_code, error=str(e))
    except Exception as e:
        logger.error("Failed to parse HTML page", state=config.state_code, error=str(e))
    return records


async def parse_newyork(content: bytes, config: StateWARNConfig) -> list[WARNRecord]:
    """Parse New York WARN notices from HTML page."""
    records: list[WARNRecord] = []
    try:
        soup = BeautifulSoup(content, "lxml")
        table = soup.find("table", {"class": re.compile(r"warn|data|notices", re.I)})
        if not table:
            for t in soup.find_all("table"):
                if t.find("th", string=re.compile(r"company|employer", re.I)):
                    table = t
                    break
        if not table:
            logger.warning("No WARN table found on NY DOL page")
            return records

        headers: list[str] = []
        header_row = table.find("tr")
        if header_row:
            headers = [
                th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])
            ]

        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            try:
                row_data: dict[str, str] = {}
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        row_data[headers[i]] = cell.get_text(strip=True)

                company_name = (
                    row_data.get("company name")
                    or row_data.get("employer")
                    or row_data.get("company")
                )
                if not company_name:
                    continue
                notice_date = parse_date(
                    row_data.get("notice date") or row_data.get("date")
                )
                if not notice_date:
                    continue
                employees = parse_employees(
                    row_data.get("number affected")
                    or row_data.get("employees")
                    or row_data.get("# affected")
                )
                naics_code, sector_category = normalize_naics(row_data.get("naics"))
                records.append(WARNRecord(
                    company_name=company_name,
                    state="NY",
                    notice_date=notice_date,
                    employees_affected=employees,
                    effective_date=parse_date(row_data.get("effective date")),
                    city=row_data.get("city") or None,
                    county=row_data.get("county") or None,
                    naics_code=naics_code,
                    sector_category=sector_category,
                    source_url=config.url,
                    raw_data=row_data,
                ))
            except Exception as e:
                logger.warning("Failed to parse NY row", error=str(e))
    except Exception as e:
        logger.error("Failed to parse NY WARN page", error=str(e))
    return records
