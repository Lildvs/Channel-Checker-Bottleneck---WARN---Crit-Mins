#!/usr/bin/env python3
"""WARN Act data backfill script.

Pulls consolidated WARN notices from layoffdata.com (Stanford Big Local News)
covering all 50 states + DC for both current year and historical data.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.data_ingestion.collectors.warn_collector import WARNCollector
from src.storage.timescale import init_database

logger = structlog.get_logger()


async def main() -> None:
    """Run WARN data backfill."""
    await init_database()

    collector = WARNCollector()

    mode = "all"
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    if mode in ("all", "current"):
        logger.info("Collecting current-year WARN data...")
        result = await collector.run_collection()
        logger.info(
            "Current-year collection done",
            success=result.success,
            records=result.records_collected,
            error=result.error_message,
        )

    if mode in ("all", "historical"):
        collector_hist = WARNCollector()
        logger.info("Collecting historical WARN data...")
        result = await collector_hist.run_backfill()
        logger.info(
            "Historical backfill done",
            success=result.success,
            records=result.records_collected,
            error=result.error_message,
        )

    logger.info("WARN backfill complete")


if __name__ == "__main__":
    asyncio.run(main())
