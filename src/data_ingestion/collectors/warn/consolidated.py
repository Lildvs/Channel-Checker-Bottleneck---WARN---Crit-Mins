"""Consolidated data source (layoffdata.com) for fallback and cross-validation."""

import csv
import io
from datetime import UTC, datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any

import httpx
import structlog

from src.data_ingestion.collectors.warn.models import WARNRecord
from src.data_ingestion.collectors.warn.parse_utils import (
    normalize_naics,
    parse_date,
    parse_employees,
)
from src.data_ingestion.collectors.warn.state_configs import (
    CONSOLIDATED_CURRENT_YEAR_URL,
    CONSOLIDATED_HISTORICAL_URL,
    STATE_NAME_TO_CODE,
)

logger = structlog.get_logger()


async def fetch_consolidated(
    client: httpx.AsyncClient,
    url: str = CONSOLIDATED_CURRENT_YEAR_URL,
    label: str = "current-year",
    states_filter: set[str] | None = None,
) -> list[WARNRecord]:
    """Fetch WARN notices from layoffdata.com consolidated CSV.

    Used as fallback for states whose direct scrapers fail and as a
    cross-validation source for scraped data.
    """
    records: list[WARNRecord] = []
    try:
        logger.info("Fetching consolidated WARN data", source=label)
        response = await client.get(url)
        response.raise_for_status()

        reader = csv.DictReader(io.StringIO(response.text))
        for row in reader:
            try:
                state_name = (row.get("State") or "").strip()
                state_code = STATE_NAME_TO_CODE.get(state_name)
                if not state_code:
                    continue
                if states_filter and state_code not in states_filter:
                    continue

                company_name = (row.get("Company") or "").strip()
                if not company_name:
                    continue
                notice_date = parse_date(row.get("WARN Received Date"))
                if not notice_date:
                    continue
                employees = parse_employees(row.get("Number of Workers"))
                effective_date = parse_date(row.get("Effective Date"))

                naics_raw = (row.get("Industry") or "").strip()
                naics_code, sector_category = normalize_naics(naics_raw)

                closure_field = (row.get("Closure / Layoff") or "").lower()
                temp_field = (row.get("Temporary/Permanent") or "").lower()
                is_closure = any(kw in closure_field for kw in ["closure", "closing"])
                is_temporary = "temporary" in temp_field or "temporary" in closure_field
                if is_closure:
                    layoff_type = "closure"
                elif is_temporary:
                    layoff_type = "furlough"
                else:
                    layoff_type = "layoff"

                records.append(WARNRecord(
                    company_name=company_name,
                    state=state_code,
                    notice_date=notice_date,
                    employees_affected=employees,
                    effective_date=effective_date,
                    city=(row.get("City") or "").strip() or None,
                    county=(row.get("County") or "").strip() or None,
                    naics_code=naics_code,
                    naics_description=naics_raw or None,
                    sector_category=sector_category,
                    layoff_type=layoff_type,
                    is_temporary=is_temporary,
                    is_closure=is_closure,
                    union_affected=(row.get("Union") or "").strip() or None,
                    notes=(row.get("Notes") or "").strip() or None,
                    source_url=url,
                    data_source="consolidated",
                    raw_data=dict(row),
                ))
            except Exception as e:
                logger.warning(
                    "Failed to parse consolidated row",
                    error=str(e),
                    company=row.get("Company"),
                )

        states_found = {r.state for r in records}
        logger.info(
            "Consolidated WARN data parsed",
            source=label,
            records=len(records),
            states=len(states_found),
        )
    except httpx.HTTPStatusError as e:
        logger.error(
            "HTTP error fetching consolidated WARN data",
            status_code=e.response.status_code,
            source=label,
        )
    except Exception as e:
        logger.error("Failed to fetch consolidated WARN data", error=str(e), source=label)

    return records


def _fuzzy_match(a: str, b: str) -> float:
    """Case-insensitive fuzzy match ratio between two strings."""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def cross_validate(
    scraped: list[dict[str, Any]],
    consolidated: list[WARNRecord],
    date_tolerance_days: int = 3,
    employee_tolerance_pct: float = 0.10,
    company_match_threshold: float = 0.80,
) -> list[dict[str, Any]]:
    """Compare scraped records against consolidated and annotate validation status.

    For each scraped record, searches for a match in consolidated data by:
    - company_name fuzzy match >= threshold
    - same state
    - notice_date within +/- tolerance days
    - employees_affected within +/- tolerance %

    Updates each scraped dict in-place with:
    - validation_status: 'validated_match' | 'validated_mismatch' | 'scraped_only'
    - validation_details: dict of field differences
    - last_validated_at: current timestamp
    """
    now = datetime.now(UTC)

    consolidated_by_state: dict[str, list[WARNRecord]] = {}
    for rec in consolidated:
        consolidated_by_state.setdefault(rec.state, []).append(rec)

    for record in scraped:
        state = record.get("state", "")
        company = record.get("company_name", "")
        notice_date = record.get("notice_date")
        employees = record.get("employees_affected", 0)

        if not notice_date or not company:
            record["validation_status"] = "scraped_only"
            record["validation_details"] = {"reason": "missing_key_fields"}
            record["last_validated_at"] = now
            continue

        candidates = consolidated_by_state.get(state, [])
        best_match = None
        best_score = 0.0

        if notice_date.tzinfo is not None:
            notice_date_naive = notice_date.replace(tzinfo=None)
        else:
            notice_date_naive = notice_date

        for cand in candidates:
            if not cand.notice_date:
                continue
            cand_date = cand.notice_date
            if cand_date.tzinfo is not None:
                cand_date = cand_date.replace(tzinfo=None)
            day_diff = abs((cand_date - notice_date_naive).days)
            if day_diff > date_tolerance_days:
                continue

            name_score = _fuzzy_match(company, cand.company_name)
            if name_score < company_match_threshold:
                continue

            if name_score > best_score:
                best_score = name_score
                best_match = cand

        if best_match is None:
            record["validation_status"] = "scraped_only"
            record["validation_details"] = {"reason": "no_consolidated_match"}
            record["last_validated_at"] = now
            continue

        diffs: dict[str, Any] = {}
        diffs["company_name_score"] = round(best_score, 3)

        best_date = best_match.notice_date
        if best_date.tzinfo is not None:
            best_date = best_date.replace(tzinfo=None)
        date_diff = abs((best_date - notice_date_naive).days)
        if date_diff > 0:
            diffs["notice_date_diff_days"] = date_diff

        if employees and best_match.employees_affected:
            emp_diff_pct = abs(employees - best_match.employees_affected) / max(employees, 1)
            if emp_diff_pct > employee_tolerance_pct:
                diffs["employees_scraped"] = employees
                diffs["employees_consolidated"] = best_match.employees_affected
                diffs["employees_diff_pct"] = round(emp_diff_pct, 3)

        if best_match.city and record.get("city"):
            if _fuzzy_match(record["city"], best_match.city) < 0.8:
                diffs["city_scraped"] = record["city"]
                diffs["city_consolidated"] = best_match.city

        has_mismatch = any(
            k in diffs for k in ["employees_diff_pct", "city_scraped"]
        ) or date_diff > 1

        record["validation_status"] = "validated_mismatch" if has_mismatch else "validated_match"
        record["validation_details"] = diffs
        record["last_validated_at"] = now

    return scraped
