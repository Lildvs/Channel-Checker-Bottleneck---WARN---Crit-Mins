#!/usr/bin/env python3
"""Database initialization script."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.storage.timescale import get_db, init_database

logger = structlog.get_logger()


async def main() -> None:
    """Initialize the database."""
    logger.info("Initializing database...")

    try:
        await init_database()
        logger.info("Database initialized successfully!")

        db = get_db()
        async with db.session() as session:
            result = await session.execute("SELECT version()")
            version = result.scalar()
            logger.info("PostgreSQL version", version=version)

    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
