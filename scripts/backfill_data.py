#!/usr/bin/env python3
"""Historical data backfill script."""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.data_ingestion.collectors import (
    BLSCollector,
    CensusCollector,
    EIACollector,
    FREDCollector,
)
from src.storage.timescale import get_db, init_database

logger = structlog.get_logger()


async def backfill_collector(
    collector,
    start_date: datetime,
    end_date: datetime,
) -> int:
    """Backfill data for a collector.

    Args:
        collector: Data collector instance
        start_date: Start date for backfill
        end_date: End date for backfill

    Returns:
        Number of records collected
    """
    logger.info(
        "Backfilling collector",
        collector=collector.name,
        start_date=start_date.date(),
        end_date=end_date.date(),
    )

    try:
        result = await collector.run_collection(
            start_date=start_date,
            end_date=end_date,
        )

        if result.success and result.data_points:
            db = get_db()
            await db.insert_data_points(result.data_points)
            logger.info(
                "Backfill complete",
                collector=collector.name,
                records=result.records_collected,
            )
            return result.records_collected
        else:
            logger.warning(
                "Backfill returned no data",
                collector=collector.name,
                error=result.error_message,
            )
            return 0

    except Exception as e:
        logger.error(
            "Backfill failed",
            collector=collector.name,
            error=str(e),
        )
        return 0


async def main() -> None:
    """Run the backfill process."""
    logger.info("Starting historical data backfill...")

    await init_database()

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=730)

    collectors = [
        FREDCollector(),
        BLSCollector(),
        EIACollector(),
        CensusCollector(),
    ]

    total_records = 0

    for collector in collectors:
        records = await backfill_collector(collector, start_date, end_date)
        total_records += records

    logger.info(
        "Backfill complete",
        total_records=total_records,
    )


if __name__ == "__main__":
    asyncio.run(main())
