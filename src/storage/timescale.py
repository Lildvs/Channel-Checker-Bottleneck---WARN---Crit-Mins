"""TimescaleDB operations for time-series data storage."""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.data_ingestion.base_collector import DataPoint
from src.storage.models import Base, DataPointModel

logger = structlog.get_logger()


class TimescaleDB:
    """Async TimescaleDB connection and operations."""

    def __init__(self, database_url: str | None = None):
        """Initialize the database connection.

        Args:
            database_url: Database URL (uses settings if not provided)
        """
        settings = get_settings()
        self.database_url = database_url or settings.database_url

        self.engine = create_async_engine(
            self.database_url,
            echo=settings.environment == "development",
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
        )

        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def init_db(self) -> None:
        """Initialize database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized")

    async def close(self) -> None:
        """Close database connections."""
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a database session."""
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def insert_data_points(
        self,
        data_points: list[DataPoint],
        batch_size: int = 1000,
    ) -> int:
        """Insert data points in batches.

        Args:
            data_points: List of data points to insert
            batch_size: Number of records per batch

        Returns:
            Number of records inserted
        """
        if not data_points:
            return 0

        total_inserted = 0

        import json as json_module

        async with self.session() as session:
            for i in range(0, len(data_points), batch_size):
                batch = data_points[i : i + batch_size]

                values = [dp.to_dict() for dp in batch]

                stmt = text("""
                    INSERT INTO data_points 
                    (id, source_id, series_id, timestamp, collected_at, value, 
                     value_text, unit, quality_score, is_preliminary, revision_number, extra_data)
                    VALUES 
                    (:id, :source_id, :series_id, :timestamp, :collected_at, :value,
                     :value_text, :unit, :quality_score, :is_preliminary, :revision_number, CAST(:extra_data AS jsonb))
                    ON CONFLICT (id, timestamp) DO UPDATE SET
                        value = EXCLUDED.value,
                        quality_score = EXCLUDED.quality_score,
                        is_preliminary = EXCLUDED.is_preliminary,
                        revision_number = EXCLUDED.revision_number,
                        collected_at = EXCLUDED.collected_at,
                        extra_data = EXCLUDED.extra_data
                """)

                for v in values:
                    v["extra_data"] = json_module.dumps(v["extra_data"])

                # Execute batch using executemany (single round-trip for entire batch)
                await session.execute(stmt, values)

                total_inserted += len(batch)

        logger.info("Inserted data points", count=total_inserted)
        return total_inserted

    async def insert_mineral_trade_flows(
        self,
        records: list[dict[str, Any]],
        batch_size: int = 500,
    ) -> int:
        """Insert mineral trade flow records into the dedicated table.

        Args:
            records: List of dicts with keys matching mineral_trade_flows columns
            batch_size: Number of records per batch

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        import json as json_module

        total_inserted = 0

        async with self.session() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                stmt = text("""
                    INSERT INTO mineral_trade_flows
                    (id, mineral, hs_code, hs_description,
                     reporter_country, reporter_iso3, partner_country, partner_iso3,
                     flow_type, value_usd, quantity, quantity_unit, weight_kg,
                     period, period_start, source, collected_at, raw_metadata)
                    VALUES
                    (:id, :mineral, :hs_code, :hs_description,
                     :reporter_country, :reporter_iso3, :partner_country, :partner_iso3,
                     :flow_type, :value_usd, :quantity, :quantity_unit, :weight_kg,
                     :period, :period_start, :source, :collected_at, CAST(:raw_metadata AS jsonb))
                    ON CONFLICT (mineral, hs_code, reporter_iso3, partner_iso3, flow_type, period)
                    DO UPDATE SET
                        value_usd = EXCLUDED.value_usd,
                        quantity = EXCLUDED.quantity,
                        weight_kg = EXCLUDED.weight_kg,
                        collected_at = EXCLUDED.collected_at,
                        raw_metadata = EXCLUDED.raw_metadata
                """)

                for rec in batch:
                    rec["raw_metadata"] = json_module.dumps(rec.get("raw_metadata", {}))

                await session.execute(stmt, batch)
                total_inserted += len(batch)

        logger.info("Inserted mineral trade flows", count=total_inserted)
        return total_inserted

    async def get_series_data(
        self,
        series_id: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        """Get time series data for a specific series.

        Args:
            series_id: The series ID to query
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Maximum number of records to return

        Returns:
            List of data point dictionaries
        """
        query = """
            SELECT id, source_id, series_id, timestamp, value, unit, 
                   quality_score, is_preliminary, extra_data
            FROM data_points
            WHERE series_id = :series_id
        """
        params: dict[str, Any] = {"series_id": series_id}

        if start_date:
            query += " AND timestamp >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND timestamp <= :end_date"
            params["end_date"] = end_date

        query += " ORDER BY timestamp DESC LIMIT :limit"
        params["limit"] = limit

        async with self.session() as session:
            result = await session.execute(text(query), params)
            rows = result.fetchall()

            return [
                {
                    "id": str(row[0]),
                    "source_id": row[1],
                    "series_id": row[2],
                    "timestamp": row[3],
                    "value": row[4],
                    "unit": row[5],
                    "quality_score": row[6],
                    "is_preliminary": row[7],
                    "extra_data": row[8],
                }
                for row in rows
            ]

    async def get_latest_value(self, series_id: str) -> dict[str, Any] | None:
        """Get the most recent value for a series.

        Args:
            series_id: The series ID to query

        Returns:
            Latest data point or None
        """
        query = """
            SELECT id, source_id, series_id, timestamp, value, unit,
                   quality_score, is_preliminary, extra_data
            FROM data_points
            WHERE series_id = :series_id
            ORDER BY timestamp DESC
            LIMIT 1
        """

        async with self.session() as session:
            result = await session.execute(text(query), {"series_id": series_id})
            row = result.fetchone()

            if not row:
                return None

            return {
                "id": str(row[0]),
                "source_id": row[1],
                "series_id": row[2],
                "timestamp": row[3],
                "value": row[4],
                "unit": row[5],
                "quality_score": row[6],
                "is_preliminary": row[7],
                "extra_data": row[8],
            }

    async def get_series_statistics(
        self,
        series_id: str,
        lookback_days: int = 90,
    ) -> dict[str, Any]:
        """Get statistical summary for a series.

        Args:
            series_id: The series ID to analyze
            lookback_days: Number of days to look back

        Returns:
            Dictionary with statistical measures
        """
        # Use make_interval for proper parameterized query (avoids SQL injection)
        query = """
            SELECT 
                COUNT(*) as count,
                AVG(value) as mean,
                STDDEV(value) as stddev,
                MIN(value) as min,
                MAX(value) as max,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median
            FROM data_points
            WHERE series_id = :series_id
              AND timestamp > NOW() - make_interval(days => :lookback_days)
              AND value IS NOT NULL
        """

        async with self.session() as session:
            result = await session.execute(
                text(query),
                {"series_id": series_id, "lookback_days": lookback_days},
            )
            row = result.fetchone()

            if not row or row[0] == 0:
                return {}

            return {
                "count": row[0],
                "mean": float(row[1]) if row[1] else None,
                "stddev": float(row[2]) if row[2] else None,
                "min": float(row[3]) if row[3] else None,
                "max": float(row[4]) if row[4] else None,
                "median": float(row[5]) if row[5] else None,
            }

    async def get_warn_monthly_aggregates(
        self,
        lookback_months: int = 6,
    ) -> list[dict[str, Any]]:
        """Get monthly WARN notice aggregates for the last N months.

        Returns rows of {month, notice_count, total_employees, closure_count}
        sorted oldest to newest.  Used by the Home dashboard WARN card.
        """
        query = """
            SELECT
                date_trunc('month', notice_date) AS month,
                COUNT(*) AS notice_count,
                COALESCE(SUM(employees_affected), 0) AS total_employees,
                COALESCE(SUM(CASE WHEN is_closure THEN 1 ELSE 0 END), 0) AS closure_count
            FROM warn_notices
            WHERE notice_date >= date_trunc('month', NOW()) - INTERVAL ':months months'
            GROUP BY date_trunc('month', notice_date)
            ORDER BY month ASC
        """
        try:
            async with self.session() as session:
                result = await session.execute(
                    text(query.replace(":months", str(lookback_months)))
                )
                rows = result.fetchall()
                return [
                    {
                        "month": row[0],
                        "notice_count": row[1],
                        "total_employees": row[2],
                        "closure_count": row[3],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error("Failed to get WARN monthly aggregates", error=str(e))
            return []

    async def get_warn_labor_pool_impact(
        self,
        lookback_months: int = 6,
    ) -> dict[str, Any]:
        """Get time-aware WARN impact for the bottleneck detector.

        Separates WARN filings into two buckets based on effective_date:

        - realized: layoffs whose effective_date <= NOW (workers already in the
          labor pool).  These are real additions to UNEMPLOY that BLS may not
          have captured yet.
        - pending:  layoffs whose effective_date > NOW (workers still employed
          but with a confirmed layoff date 60-90 days out).  These represent
          predictable future labor-pool inflation.
        - unknown:  effective_date is NULL (state did not report it).  Treated
          conservatively as realized if the notice_date is > 90 days old,
          otherwise as pending.

        The 90-day cutoff for unknown records is because the WARN Act requires
        60 days minimum notice; adding a 30-day buffer captures most cases.
        """
        query = """
            SELECT
                COALESCE(SUM(
                    CASE WHEN effective_date IS NOT NULL AND effective_date <= NOW()
                         THEN employees_affected ELSE 0 END
                ), 0) AS realized_employees,
                COALESCE(SUM(
                    CASE WHEN effective_date IS NOT NULL AND effective_date > NOW()
                         THEN employees_affected ELSE 0 END
                ), 0) AS pending_employees,
                COALESCE(SUM(
                    CASE WHEN effective_date IS NULL AND notice_date <= NOW() - INTERVAL '90 days'
                         THEN employees_affected ELSE 0 END
                ), 0) AS unknown_old_employees,
                COALESCE(SUM(
                    CASE WHEN effective_date IS NULL AND notice_date > NOW() - INTERVAL '90 days'
                         THEN employees_affected ELSE 0 END
                ), 0) AS unknown_recent_employees,
                COUNT(*) FILTER (WHERE effective_date IS NOT NULL AND effective_date <= NOW())
                    AS realized_notices,
                COUNT(*) FILTER (WHERE effective_date IS NOT NULL AND effective_date > NOW())
                    AS pending_notices,
                COUNT(*) FILTER (WHERE effective_date IS NULL)
                    AS unknown_notices,
                COUNT(*) AS total_notices,
                COALESCE(SUM(employees_affected), 0) AS total_employees
            FROM warn_notices
            WHERE notice_date >= date_trunc('month', NOW()) - INTERVAL ':months months'
        """
        try:
            async with self.session() as session:
                result = await session.execute(
                    text(query.replace(":months", str(lookback_months)))
                )
                row = result.fetchone()
                if row is None:
                    return {}

                realized = int(row[0]) + int(row[2])
                pending = int(row[1]) + int(row[3])

                return {
                    "realized_employees": realized,
                    "pending_employees": pending,
                    "realized_notices": int(row[4]) + int(row[6]),
                    "pending_notices": int(row[5]),
                    "total_notices": int(row[7]),
                    "total_employees": int(row[8]),
                }
        except Exception as e:
            logger.error("Failed to get WARN labor pool impact", error=str(e))
            return {}

    async def save_warn_notices(
        self,
        records: list[dict[str, Any]],
        batch_size: int = 100,
    ) -> int:
        """Save WARN notice records to the database.

        Uses upsert (INSERT ... ON CONFLICT DO UPDATE) to handle
        duplicate notices gracefully.

        Args:
            records: List of WARN record dictionaries
            batch_size: Number of records per batch

        Returns:
            Number of records inserted/updated
        """
        if not records:
            return 0

        total_inserted = 0
        import json as json_module

        async with self.session() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                stmt = text("""
                    INSERT INTO warn_notices 
                    (id, company_name, company_address, city, state, zip_code, county,
                     notice_date, effective_date, employees_affected, layoff_type,
                     naics_code, naics_description, sector_category,
                     is_temporary, is_closure, union_affected, reason, notes,
                     source_state, source_url, data_source, collected_at, raw_data)
                    VALUES 
                    (gen_random_uuid(), :company_name, :company_address, :city, :state, 
                     :zip_code, :county, :notice_date, :effective_date, :employees_affected,
                     :layoff_type, :naics_code, :naics_description, :sector_category,
                     :is_temporary, :is_closure, :union_affected, :reason, :notes,
                     :source_state, :source_url, :data_source, NOW(), CAST(:raw_data AS jsonb))
                    ON CONFLICT (company_name, state, notice_date, employees_affected) 
                    DO UPDATE SET
                        effective_date = COALESCE(EXCLUDED.effective_date, warn_notices.effective_date),
                        naics_code = COALESCE(EXCLUDED.naics_code, warn_notices.naics_code),
                        naics_description = COALESCE(EXCLUDED.naics_description, warn_notices.naics_description),
                        sector_category = COALESCE(EXCLUDED.sector_category, warn_notices.sector_category),
                        is_temporary = EXCLUDED.is_temporary,
                        is_closure = EXCLUDED.is_closure,
                        reason = COALESCE(EXCLUDED.reason, warn_notices.reason),
                        data_source = EXCLUDED.data_source,
                        raw_data = EXCLUDED.raw_data
                """)

                all_params = []
                for record in batch:
                    all_params.append({
                        "company_name": record.get("company_name", "Unknown"),
                        "company_address": record.get("company_address"),
                        "city": record.get("city"),
                        "state": record.get("state", "XX"),
                        "zip_code": record.get("zip_code"),
                        "county": record.get("county"),
                        "notice_date": record.get("notice_date"),
                        "effective_date": record.get("effective_date"),
                        "employees_affected": record.get("employees_affected", 0),
                        "layoff_type": record.get("layoff_type", "layoff"),
                        "naics_code": record.get("naics_code"),
                        "naics_description": record.get("naics_description"),
                        "sector_category": record.get("sector_category"),
                        "is_temporary": record.get("is_temporary", False),
                        "is_closure": record.get("is_closure", False),
                        "union_affected": record.get("union_affected"),
                        "reason": record.get("reason"),
                        "notes": record.get("notes"),
                        "source_state": record.get("source_state", record.get("state", "XX")),
                        "source_url": record.get("source_url"),
                        "data_source": record.get("data_source", "scraped"),
                        "raw_data": json_module.dumps(record.get("raw_data", {})),
                    })

                # Execute batch using executemany (single round-trip for entire batch)
                await session.execute(stmt, all_params)

                total_inserted += len(batch)

        logger.info("Saved WARN notices", count=total_inserted)
        return total_inserted


# Global database instance
_db: TimescaleDB | None = None


def get_db() -> TimescaleDB:
    """Get the global database instance."""
    global _db
    if _db is None:
        _db = TimescaleDB()
    return _db


async def init_database() -> None:
    """Initialize the database."""
    db = get_db()
    await db.init_db()
