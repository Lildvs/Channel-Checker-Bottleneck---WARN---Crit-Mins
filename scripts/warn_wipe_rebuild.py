#!/usr/bin/env python3
"""Wipe warn_notices table, run migration, fresh-scrape all states, cross-validate.

Usage:
    DB_HOST=localhost REDIS_HOST=localhost python scripts/warn_wipe_rebuild.py [--skip-wipe] [--skip-cross-validate]

Steps:
1. Run migration 002 (add new columns + scraper_health table)
2. TRUNCATE warn_notices (unless --skip-wipe)
3. Collect WARN data for all 50 states + DC (government-first with consolidated fallback)
4. Run cross-validation pass (unless --skip-cross-validate)
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.data_ingestion.collectors.warn.collector import WARNCollector
from src.storage.timescale import get_db, init_database

logger = structlog.get_logger()

MIGRATION_FILE = Path(__file__).parent / "migrations" / "002_warn_validation_columns.sql"


async def run_migration() -> None:
    """Execute migration 002 to add validation columns and scraper_health table.

    asyncpg does not support multiple statements in a single prepared
    statement, so we execute each DDL statement individually via
    raw asyncpg connection to bypass prepared-statement limitations.
    """
    db = get_db()
    logger.info("Running migration 002 via raw SQL...")

    ddl_statements = [
        "ALTER TABLE warn_notices ADD COLUMN IF NOT EXISTS data_source VARCHAR(20) NOT NULL DEFAULT 'scraped'",
        "ALTER TABLE warn_notices ADD COLUMN IF NOT EXISTS validation_status VARCHAR(30)",
        "ALTER TABLE warn_notices ADD COLUMN IF NOT EXISTS validation_details JSONB",
        "ALTER TABLE warn_notices ADD COLUMN IF NOT EXISTS last_validated_at TIMESTAMPTZ",
        "CREATE INDEX IF NOT EXISTS idx_warn_notices_data_source ON warn_notices (data_source)",
        "CREATE INDEX IF NOT EXISTS idx_warn_notices_validation ON warn_notices (validation_status)",
    ]

    from sqlalchemy import text

    async with db.session() as session:
        for stmt_str in ddl_statements:
            logger.info("Executing", sql=stmt_str[:80])
            await session.execute(text(stmt_str))
        await session.commit()
    logger.info("Migration 002 complete")


async def wipe_table() -> None:
    """Truncate the warn_notices table."""
    db = get_db()
    logger.warning("TRUNCATING warn_notices table...")
    async with db.session() as session:
        from sqlalchemy import text
        await session.execute(text("TRUNCATE warn_notices"))
        await session.commit()
    logger.info("warn_notices table truncated")


async def collect_all_states() -> int:
    """Run collection for all 50 states + DC."""
    collector = WARNCollector()
    logger.info("Starting full WARN collection for all states...")
    result = await collector.run_collection()
    logger.info(
        "Collection complete",
        success=result.success,
        records=result.records_collected,
        error=result.error_message,
        metadata=result.metadata,
    )
    return result.records_collected


async def run_cross_validation() -> None:
    """Run cross-validation comparing scraped vs consolidated data."""
    collector = WARNCollector()
    logger.info("Starting cross-validation pass...")
    result = await collector.run_cross_validation()
    logger.info("Cross-validation complete", result=result)


async def main(skip_wipe: bool = False, skip_cross_validate: bool = False) -> None:
    await init_database()

    await run_migration()

    if not skip_wipe:
        await wipe_table()

    records = await collect_all_states()

    if not skip_cross_validate and records > 0:
        await run_cross_validation()

    logger.info("WARN wipe & rebuild complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wipe and rebuild WARN notices")
    parser.add_argument("--skip-wipe", action="store_true", help="Skip truncating the table")
    parser.add_argument("--skip-cross-validate", action="store_true", help="Skip cross-validation")
    args = parser.parse_args()
    asyncio.run(main(skip_wipe=args.skip_wipe, skip_cross_validate=args.skip_cross_validate))
