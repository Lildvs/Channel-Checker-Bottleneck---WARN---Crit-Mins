"""WARN Act layoff notice collector -- government-first with consolidated fallback.

Strategy per state:
1. Try direct government website scraper
2. On failure, fall back to layoffdata.com consolidated data for that state
3. Tag every record with data_source ('scraped' or 'consolidated')
4. Cross-validate scraped records against consolidated data
5. Track per-state scraper health and alert on repeated failures
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import structlog

from src.data_ingestion.base_collector import BaseCollector, CollectionResult, DataFrequency
from src.data_ingestion.collectors.warn import consolidated as consolidated_mod
from src.data_ingestion.collectors.warn import health as health_mod
from src.data_ingestion.collectors.warn.csv_parsers import parse_oregon_csv
from src.data_ingestion.collectors.warn.excel_parsers import parse_california, parse_texas
from src.data_ingestion.collectors.warn.html_parsers import parse_generic_html, parse_newyork
from src.data_ingestion.collectors.warn.models import StateWARNConfig, WARNRecord
from src.data_ingestion.collectors.warn.parse_utils import trunc
from src.data_ingestion.collectors.warn.pdf_parsers import parse_idaho_pdf, parse_newmexico_pdf
from src.data_ingestion.collectors.warn.state_configs import JS_FALLBACK_STATES, STATE_CONFIGS
from src.storage.timescale import get_db

logger = structlog.get_logger()

PARSER_REGISTRY: dict[str, Any] = {
    "parse_california": parse_california,
    "parse_texas": parse_texas,
    "parse_newyork": parse_newyork,
    "parse_generic_html": parse_generic_html,
    "parse_oregon_csv": parse_oregon_csv,
    "parse_idaho_pdf": parse_idaho_pdf,
    "parse_newmexico_pdf": parse_newmexico_pdf,
}


class WARNCollector(BaseCollector):
    """Government-first WARN collector with consolidated fallback."""

    def __init__(
        self,
        data_dir: Path | None = None,
        states: list[str] | None = None,
        timeout: float = 60.0,
        max_retries: int = 3,
    ):
        super().__init__(name="WARN Collector", source_id="warn")
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw" / "warn"
        self.timeout = timeout
        self.max_retries = max_retries

        if states:
            self.states = {k: v for k, v in STATE_CONFIGS.items() if k in states}
        else:
            self.states = {k: v for k, v in STATE_CONFIGS.items() if v.enabled}

        self._client: httpx.AsyncClient | None = None
        self._last_records: list[WARNRecord] = []

    @property
    def frequency(self) -> DataFrequency:
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        return "0 12 * * *"

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
                headers={
                    "User-Agent": "ChannelCheckResearcher/1.0 (Economic Research Tool)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def get_default_series(self) -> list[str]:
        return list(self.states.keys())

    async def _fetch_and_parse_state(
        self,
        config: StateWARNConfig,
        client: httpx.AsyncClient,
    ) -> list[WARNRecord]:
        """Attempt to scrape a single state's government site."""
        parser_fn = PARSER_REGISTRY.get(config.parser)
        if not parser_fn:
            logger.warning("No parser registered", state=config.state_code, parser=config.parser)
            return []

        for attempt in range(self.max_retries):
            try:
                logger.info(
                    "Fetching WARN data",
                    state=config.state_code,
                    url=config.url,
                    attempt=attempt + 1,
                )
                response = await client.get(config.url)
                response.raise_for_status()

                records = await parser_fn(response.content, config)
                for r in records:
                    r.data_source = "scraped"

                logger.info(
                    "Parsed WARN records",
                    state=config.state_code,
                    count=len(records),
                )
                await self._save_raw_file(config, response.content)
                return records

            except httpx.HTTPStatusError as e:
                logger.warning(
                    "HTTP error fetching WARN data",
                    state=config.state_code,
                    status_code=e.response.status_code,
                    attempt=attempt + 1,
                )
            except httpx.RequestError as e:
                logger.warning(
                    "Request error fetching WARN data",
                    state=config.state_code,
                    error=str(e),
                    attempt=attempt + 1,
                )
            except Exception as e:
                logger.error(
                    "Unexpected error fetching WARN data",
                    state=config.state_code,
                    error=str(e),
                    attempt=attempt + 1,
                )

        logger.error(
            "Failed to fetch WARN data after retries",
            state=config.state_code,
            max_retries=self.max_retries,
        )
        return []

    async def _save_raw_file(self, config: StateWARNConfig, content: bytes) -> Path:
        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        save_dir = self.raw_dir / config.state_code / date_str
        save_dir.mkdir(parents=True, exist_ok=True)
        ext_map = {"excel": "xlsx", "pdf": "pdf", "csv": "csv"}
        ext = ext_map.get(config.format, "html")
        file_path = save_dir / f"warn_{config.state_code}_{date_str}.{ext}"
        file_path.write_bytes(content)
        return file_path

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[WARNRecord]:
        """Collect WARN notices: scrape-first per state, fall back to consolidated.

        For each state:
        1. If JS fallback state, skip direct scrape
        2. Try direct government site scraper
        3. On success, record health; on failure, fall back to consolidated for that state
        4. Tag all records with data_source
        """
        client = await self._get_client()
        all_records: list[WARNRecord] = []

        target_states = set(series_ids) if series_ids else set(self.states.keys())
        failed_states: set[str] = set()

        for state_code in sorted(target_states):
            config = self.states.get(state_code)
            if not config:
                continue

            if config.format == "js_fallback":
                failed_states.add(state_code)
                continue

            try:
                records = await self._fetch_and_parse_state(config, client)
                if records:
                    all_records.extend(records)
                    await health_mod.record_success(state_code, len(records))
                else:
                    failed_states.add(state_code)
                    await health_mod.record_failure(state_code, "Parser returned 0 records")
            except Exception as e:
                failed_states.add(state_code)
                await health_mod.record_failure(state_code, str(e))
                logger.error("State collection failed", state=state_code, error=str(e))

        if failed_states:
            logger.info(
                "Falling back to consolidated for failed states",
                states=sorted(failed_states),
            )
            try:
                consolidated_records = await consolidated_mod.fetch_consolidated(
                    client,
                    states_filter=failed_states,
                )
                all_records.extend(consolidated_records)
                consolidated_states = {r.state for r in consolidated_records}
                for sc in consolidated_states:
                    await health_mod.record_success(sc, sum(1 for r in consolidated_records if r.state == sc))
            except Exception as e:
                logger.error("Consolidated fallback failed", error=str(e))

        # Deduplicate
        seen: set[tuple[str, str, str, int]] = set()
        deduped: list[WARNRecord] = []
        for record in all_records:
            key = (
                record.company_name.lower().strip(),
                record.state,
                record.notice_date.strftime("%Y-%m-%d") if record.notice_date else "",
                record.employees_affected,
            )
            if key not in seen:
                seen.add(key)
                deduped.append(record)

        if start_date:
            deduped = [r for r in deduped if r.notice_date >= start_date]
        if end_date:
            deduped = [r for r in deduped if r.notice_date <= end_date]

        scraped_count = sum(1 for r in deduped if r.data_source == "scraped")
        consolidated_count = sum(1 for r in deduped if r.data_source == "consolidated")
        states_collected = {r.state for r in deduped}

        logger.info(
            "WARN collection complete",
            total_records=len(deduped),
            states_with_data=len(states_collected),
            scraped=scraped_count,
            consolidated_fallback=consolidated_count,
            failed_states=sorted(failed_states),
        )

        self._last_records = deduped
        return deduped

    async def run_collection(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> CollectionResult:
        """Run a full WARN collection cycle with persistence."""
        started_at = datetime.now(UTC)
        try:
            records = await self.collect(series_ids, start_date, end_date)
            saved = 0
            if records:
                db = get_db()
                dicts = self.records_to_dicts(records)
                saved = await db.save_warn_notices(dicts)
                states_with_data = {r.state for r in records}
                logger.info(
                    "WARN notices persisted",
                    saved=saved,
                    states=len(states_with_data),
                    state_codes=sorted(states_with_data),
                )

            return CollectionResult(
                collector_name=self.name,
                started_at=started_at,
                completed_at=datetime.now(UTC),
                success=True,
                records_collected=len(records),
                data_points=[],
                metadata={
                    "states_with_data": sorted({r.state for r in records}),
                    "warn_records": len(records),
                    "saved_to_db": saved,
                },
            )
        except Exception as e:
            logger.error("WARN collection failed", error=str(e))
            return CollectionResult(
                collector_name=self.name,
                started_at=started_at,
                completed_at=datetime.now(UTC),
                success=False,
                error_message=str(e),
            )
        finally:
            await self.close()

    async def run_cross_validation(self) -> dict[str, int]:
        """Run cross-validation comparing scraped records against consolidated.

        This is a post-collection step that should be called separately
        from the main collect() flow to avoid blocking scraping.
        """
        from sqlalchemy import text as sa_text
        import json as json_mod

        client = await self._get_client()
        try:
            consolidated_records = await consolidated_mod.fetch_consolidated(client)
            db = get_db()

            async with db.session() as session:
                result = await session.execute(sa_text(
                    "SELECT * FROM warn_notices WHERE data_source = 'scraped' "
                    "AND (validation_status IS NULL OR validation_status = 'unvalidated')"
                ))
                rows = result.fetchall()
                columns = result.keys()

            if not rows:
                return {"validated": 0, "match": 0, "mismatch": 0, "scraped_only": 0}

            scraped_dicts = [dict(zip(columns, row)) for row in rows]
            consolidated_mod.cross_validate(scraped_dicts, consolidated_records)

            match_count = sum(1 for r in scraped_dicts if r.get("validation_status") == "validated_match")
            mismatch_count = sum(1 for r in scraped_dicts if r.get("validation_status") == "validated_mismatch")
            scraped_only = sum(1 for r in scraped_dicts if r.get("validation_status") == "scraped_only")

            update_stmt = sa_text(
                "UPDATE warn_notices SET validation_status = :vstatus, "
                "validation_details = CAST(:vdetails AS jsonb), last_validated_at = :vlast "
                "WHERE id = :vid"
            )
            async with db.session() as session:
                for record in scraped_dicts:
                    await session.execute(update_stmt, {
                        "vstatus": record.get("validation_status", "unvalidated"),
                        "vdetails": json_mod.dumps(record.get("validation_details") or {}),
                        "vlast": record.get("last_validated_at"),
                        "vid": record["id"],
                    })
                await session.commit()

            logger.info(
                "Cross-validation complete",
                total=len(scraped_dicts),
                match=match_count,
                mismatch=mismatch_count,
                scraped_only=scraped_only,
            )
            return {
                "validated": len(scraped_dicts),
                "match": match_count,
                "mismatch": mismatch_count,
                "scraped_only": scraped_only,
            }
        except Exception as e:
            logger.error("Cross-validation failed", error=str(e))
            return {"error": str(e)}
        finally:
            await self.close()

    def records_to_dicts(self, records: list[WARNRecord]) -> list[dict[str, Any]]:
        """Convert WARN records to dicts for database insertion.

        Truncates varchar fields to their DB column limits.
        """
        t = trunc
        return [
            {
                "company_name": t(r.company_name, 500),
                "state": r.state,
                "notice_date": r.notice_date,
                "employees_affected": r.employees_affected,
                "effective_date": r.effective_date,
                "company_address": r.company_address,
                "city": t(r.city, 300),
                "zip_code": t(r.zip_code, 10),
                "county": t(r.county, 200),
                "naics_code": t(r.naics_code, 10),
                "naics_description": t(r.naics_description, 500),
                "sector_category": t(r.sector_category, 50),
                "layoff_type": t(r.layoff_type, 50),
                "is_temporary": r.is_temporary,
                "is_closure": r.is_closure,
                "union_affected": t(r.union_affected, 500),
                "reason": r.reason,
                "notes": r.notes,
                "source_state": r.state,
                "source_url": r.source_url,
                "data_source": r.data_source,
                "raw_data": r.raw_data,
            }
            for r in records
        ]
