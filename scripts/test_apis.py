#!/usr/bin/env python3
"""API connection test script."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.config.settings import get_settings
from src.data_ingestion.collectors import (
    BLSCollector,
    CensusCollector,
    EIACollector,
    FREDCollector,
    GDELTCollector,
    GoogleTrendsCollector,
)

logger = structlog.get_logger()


async def test_collector(collector, name: str) -> bool:
    """Test a collector's API connection.

    Args:
        collector: Collector instance
        name: Name for logging

    Returns:
        True if successful
    """
    try:
        is_valid = await collector.validate_api_key()
        if is_valid:
            logger.info(f"✅ {name}: API connection successful")
            return True
        else:
            logger.warning(f"❌ {name}: API validation failed")
            return False
    except Exception as e:
        logger.error(f"❌ {name}: Error - {str(e)}")
        return False


async def main() -> None:
    """Test all API connections."""
    logger.info("Testing API connections...")

    settings = get_settings()

    logger.info("Configured API keys:")
    logger.info(f"  FRED: {'✓' if settings.fred_api_key else '✗'}")
    logger.info(f"  BLS: {'✓' if settings.bls_api_key else '✗'}")
    logger.info(f"  BEA: {'✓' if settings.bea_api_key else '✗'}")
    logger.info(f"  EIA: {'✓' if settings.eia_api_key else '✗'}")
    logger.info(f"  Census: {'✓' if settings.census_api_key else '✗'}")

    collectors = [
        (FREDCollector(), "FRED"),
        (BLSCollector(), "BLS"),
        (EIACollector(), "EIA"),
        (CensusCollector(), "Census"),
        (GDELTCollector(), "GDELT (no key required)"),
        (GoogleTrendsCollector(), "Google Trends (no key required)"),
    ]

    results = []
    for collector, name in collectors:
        result = await test_collector(collector, name)
        results.append((name, result))

    logger.info("\n=== Summary ===")
    passed = sum(1 for _, r in results if r)
    failed = sum(1 for _, r in results if not r)
    logger.info(f"Passed: {passed}/{len(results)}")
    logger.info(f"Failed: {failed}/{len(results)}")

    if failed > 0:
        logger.warning("\nSome API connections failed. Check your .env configuration.")
        sys.exit(1)
    else:
        logger.info("\nAll API connections successful!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
