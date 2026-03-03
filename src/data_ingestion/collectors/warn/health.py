"""Scraper health tracking and alerting for per-state WARN collection."""

from datetime import UTC, datetime

import structlog
from sqlalchemy import text

from src.storage.timescale import get_db

logger = structlog.get_logger()

CONSECUTIVE_FAILURE_THRESHOLD = 3


async def record_success(state_code: str, record_count: int) -> None:
    """Record a successful scrape for a state."""
    db = get_db()
    query = text("""
        INSERT INTO scraper_health (state, last_success_at, total_runs, total_successes,
                                     last_record_count, consecutive_failures, status)
        VALUES (:state, :now, 1, 1, :count, 0, 'healthy')
        ON CONFLICT (state) DO UPDATE SET
            last_success_at = :now,
            total_runs = scraper_health.total_runs + 1,
            total_successes = scraper_health.total_successes + 1,
            last_record_count = :count,
            consecutive_failures = 0,
            status = 'healthy'
    """)
    now = datetime.now(UTC)
    try:
        async with db.session() as session:
            await session.execute(query, {"state": state_code, "now": now, "count": record_count})
            await session.commit()
    except Exception as e:
        logger.error("Failed to record scraper success", state=state_code, error=str(e))


async def record_failure(state_code: str, error_msg: str) -> None:
    """Record a scraper failure and potentially generate an alert."""
    db = get_db()
    now = datetime.now(UTC)

    upsert_query = text("""
        INSERT INTO scraper_health (state, last_failure_at, last_error, total_runs,
                                     total_successes, consecutive_failures, status)
        VALUES (:state, :now, :error, 1, 0, 1, 'degraded')
        ON CONFLICT (state) DO UPDATE SET
            last_failure_at = :now,
            last_error = :error,
            total_runs = scraper_health.total_runs + 1,
            consecutive_failures = scraper_health.consecutive_failures + 1,
            status = CASE
                WHEN scraper_health.consecutive_failures + 1 >= :threshold THEN 'failing'
                ELSE 'degraded'
            END
    """)
    try:
        async with db.session() as session:
            await session.execute(upsert_query, {
                "state": state_code, "now": now, "error": error_msg,
                "threshold": CONSECUTIVE_FAILURE_THRESHOLD,
            })
            await session.commit()
    except Exception as e:
        logger.error("Failed to record scraper failure", state=state_code, error=str(e))
        return

    consecutive = await _get_consecutive_failures(state_code)
    if consecutive >= CONSECUTIVE_FAILURE_THRESHOLD:
        logger.warning(
            "scraper_health_alert",
            state=state_code,
            consecutive_failures=consecutive,
            last_error=error_msg[:500],
            alert_type="consecutive_failures",
        )


async def _get_consecutive_failures(state_code: str) -> int:
    db = get_db()
    try:
        async with db.session() as session:
            result = await session.execute(
                text("SELECT consecutive_failures FROM scraper_health WHERE state = :state"),
                {"state": state_code},
            )
            row = result.fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


async def get_health_summary() -> list[dict]:
    """Get health status for all tracked states."""
    db = get_db()
    try:
        async with db.session() as session:
            result = await session.execute(text("SELECT * FROM scraper_health ORDER BY state"))
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error("Failed to get health summary", error=str(e))
        return []
