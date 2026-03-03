"""APScheduler-based scheduler for data collection jobs.

Implements frequency-based collection and retention:
- Daily sources: Collected daily, 1 year raw retention OR 250 GB threshold
- Weekly sources: Collected weekly, 2 years raw retention
- Monthly sources: Collected monthly, 5 years raw retention
- Quarterly/Annual: Collected per schedule, 8 years raw retention
- Irregular sources: Checked daily, archive old when new arrives
- All archives: 8 years before permanent deletion
"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.config.settings import get_settings
from src.config.data_frequencies import (
    DATA_SOURCES,
    DataFrequency,
    get_data_source,
    get_daily_sources,
)
from src.data_ingestion.base_collector import BaseCollector, CollectionResult
from src.data_ingestion.collectors import (
    BLSCollector,
    BNPLSECCollector,
    CensusCollector,
    EIACollector,
    FREDCollector,
    GDELTCollector,
    GoogleTrendsCollector,
    NFIBCollector,
    PortCongestionCollector,
    USATradeCollector,
    VAHealthcareCollector,
    ResearchCollector,
    WARNCollector,
)
from src.data_ingestion.collectors.bea_io_collector import BEAIOCollector
from src.data_ingestion.collectors.sec_edgar_collector import SECEdgarCollector
from src.data_ingestion.collectors.commodity_inventory_collector import CommodityInventoryCollector
from src.data_ingestion.collectors.shipping_collector import ShippingDataCollector
from src.data_ingestion.collectors.critical_minerals_collector import CriticalMineralsCollector
from src.data_ingestion.collectors.warn.state_configs import STATE_CONFIGS as WARN_STATE_CONFIGS
from src.storage.archive_manager import ArchiveManager
from src.storage.redis_cache import get_cache, init_cache
from src.storage.timescale import get_db, init_database

logger = structlog.get_logger()


def _warn_states_for_tier(tier: str) -> list[str]:
    """Return state codes for a given WARN schedule tier."""
    return [
        code for code, cfg in WARN_STATE_CONFIGS.items()
        if cfg.schedule_tier == tier and cfg.enabled
    ]


COLLECTION_SCHEDULES: dict[str, dict[str, Any]] = {
    "gdelt_realtime": {
        "collector": "gdelt",
        "interval_minutes": 15,
        "frequency": "real_time",
        "description": "GDELT news sentiment every 15 min",
    },
    "trends_daily": {
        "collector": "trends",
        "cron": "0 3 * * *",  # Daily at 3 AM UTC (low-rate to avoid blocking)
        "frequency": "daily",
        "description": "Google Trends daily collection (general + 250 stress keywords)",
    },
    "fred_daily": {
        "collector": "fred",
        "cron": "0 11 * * *",  # 6 AM ET (11 UTC)
        "frequency": "daily",
        "description": "Daily FRED data collection (Baltic indices, etc.)",
    },
    "eia_weekly": {
        "collector": "eia",
        "cron": "30 15 * * 3",  # Wednesday 10:30 AM ET
        "frequency": "weekly",
        "description": "Weekly EIA petroleum/natural gas data",
    },
    "warn_daily": {
        "collector": "warn_daily",
        "cron": "0 12 * * *",  # Daily at noon UTC
        "frequency": "daily",
        "description": "WARN notices - daily tier (CA, TX, NY, FL, PA, IL, OH)",
    },
    "warn_twice_weekly": {
        "collector": "warn_twice_weekly",
        "cron": "0 12 * * 1,4",  # Monday and Thursday at noon UTC
        "frequency": "weekly",
        "description": "WARN notices - twice-weekly tier (GA, NC, MI, NJ, VA, WA, etc.)",
    },
    "warn_weekly": {
        "collector": "warn_weekly",
        "cron": "0 8 * * 3",  # Wednesday at 8 AM UTC
        "frequency": "weekly",
        "description": "WARN notices - weekly tier (remaining states)",
    },
    "usa_trade_weekly": {
        "collector": "usa_trade",
        "cron": "0 7 * * 2",  # Every Tuesday at 7 AM UTC
        "frequency": "weekly",
        "description": "USA Trade Online data check (monthly source)",
    },
    "bls_monthly": {
        "collector": "bls",
        "cron": "30 13 1-10 * *",  # First 10 days of month, 8:30 AM ET
        "frequency": "monthly",
        "description": "Monthly BLS employment data collection",
    },
    "census_monthly": {
        "collector": "census",
        "cron": "0 13 15 * *",  # 15th of month, 8 AM ET
        "frequency": "monthly",
        "description": "Monthly Census economic data",
    },
    "va_healthcare_quarterly": {
        "collector": "va_healthcare",
        "cron": "0 6 * * 1",  # Check weekly (source updates quarterly)
        "frequency": "quarterly",
        "description": "VA healthcare data check (quarterly source)",
    },
    "bea_io_annual": {
        "collector": "bea_io",
        "cron": "0 14 1 10 *",  # October 1st, 2 PM UTC (after September release)
        "frequency": "annual",
        "description": "BEA Input-Output Tables (annual, September release)",
    },
    "research_intelligence": {
        "collector": "research",
        "cron": "0 */6 * * *",  # Every 6 hours
        "frequency": "daily",
        "description": "Research paper collection from arXiv, Semantic Scholar, OpenAlex, PubMed",
    },
    "nfib_monthly": {
        "collector": "nfib",
        "cron": "0 14 15 * *",  # 15th of month, 2 PM UTC
        "frequency": "monthly",
        "description": "NFIB Small Business Optimism Index (monthly)",
    },
    "port_congestion_weekly": {
        "collector": "port_congestion",
        "cron": "0 5 * * 2",  # Tuesday at 5 AM UTC
        "frequency": "weekly",
        "description": "Port congestion from 4 sources (GoComet runs last, credit-managed)",
    },
    "bnpl_sec_weekly": {
        "collector": "bnpl_sec",
        "cron": "0 1 * * 1",  # Monday at 1 AM UTC
        "frequency": "weekly",
        "description": "BNPL delinquency data from Affirm/Klarna SEC filings",
    },
    "sec_edgar_daily": {
        "collector": "sec_edgar",
        "cron": "0 22 * * 1-5",  # Weekdays at 10 PM UTC (after market close)
        "frequency": "daily",
        "description": "SEC EDGAR risk-factor keyword filings",
    },
    "commodity_inventory_weekly": {
        "collector": "commodity_inventory",
        "cron": "0 16 * * 4",  # Thursday at 4 PM UTC
        "frequency": "weekly",
        "description": "Commodity inventory data (EIA, NASS, LME/COMEX)",
    },
    "shipping_monthly": {
        "collector": "shipping",
        "cron": "0 14 16 * *",  # 16th of month, 2 PM UTC
        "frequency": "monthly",
        "description": "Port of LA TEU + BTS port performance PDF",
    },
    "critical_minerals_weekly": {
        "collector": "critical_minerals",
        "cron": "0 9 * * 1",  # Monday at 9 AM UTC
        "frequency": "weekly",
        "description": "Critical minerals trade flows + USGS/IEA data",
    },
}

COLLECTOR_FREQUENCIES: dict[str, str] = {
    "fred": "daily",
    "bls": "monthly",
    "eia": "weekly",
    "census": "monthly",
    "gdelt": "real_time",
    "trends": "real_time",
    "va_healthcare": "quarterly",
    "usa_trade": "monthly",
    "research": "daily",
    "warn_daily": "daily",
    "warn_twice_weekly": "weekly",
    "warn_weekly": "weekly",
    "bea_io": "annual",
    "sec_edgar": "daily",
    "commodity_inventory": "weekly",
    "shipping": "monthly",
    "critical_minerals": "monthly",
}

FILE_COLLECTOR_CONFIGS: dict[str, dict[str, Any]] = {
    "va_healthcare": {
        "check_interval_hours": 168,  # Check weekly (source updates quarterly)
        "expected_update": "quarterly",
        "frequency": "quarterly",
    },
    "usa_trade": {
        "check_interval_hours": 168,  # Check weekly
        "expected_update": "monthly",
        "frequency": "monthly",
    },
}


class DataCollectionScheduler:
    """Scheduler for automated data collection with frequency-based retention.
    
    Retention Policy:
    - Daily data: 1 year raw, OR 250 GB threshold
    - Weekly data: 2 years raw
    - Monthly data: 5 years raw
    - Quarterly/Annual data: 8 years raw
    - All archives: 8 years before permanent deletion
    """

    ARCHIVE_RETENTION_YEARS = 8  # Keep compressed archives for 8 years

    def __init__(self, data_dir: Path | None = None):
        """Initialize the scheduler.

        Args:
            data_dir: Base data directory for file-based collectors
        """
        self.scheduler = AsyncIOScheduler()
        self.collectors: dict[str, BaseCollector] = {}
        self.data_dir = data_dir or Path("data")
        self.archive_manager = ArchiveManager(
            data_dir=self.data_dir,
            retention_years=self.ARCHIVE_RETENTION_YEARS,
        )
        self.logger = logger.bind(component="scheduler")
        self._init_collectors()

    def _init_collectors(self) -> None:
        """Initialize all data collectors."""
        self.collectors = {
            "fred": FREDCollector(),
            "bls": BLSCollector(),
            "eia": EIACollector(),
            "census": CensusCollector(),
            "gdelt": GDELTCollector(),
            "trends": GoogleTrendsCollector(),
            "va_healthcare": VAHealthcareCollector(data_dir=self.data_dir),
            "usa_trade": USATradeCollector(data_dir=self.data_dir),
            "research": ResearchCollector(
                lookback_days=7,
                max_per_source=100,
                topics=None,  # Collect all topics, user filters downstream
            ),
            "warn_daily": WARNCollector(
                data_dir=self.data_dir,
                states=_warn_states_for_tier("daily"),
            ),
            "warn_twice_weekly": WARNCollector(
                data_dir=self.data_dir,
                states=_warn_states_for_tier("twice_weekly"),
            ),
            "warn_weekly": WARNCollector(
                data_dir=self.data_dir,
                states=_warn_states_for_tier("weekly"),
            ),
            "bea_io": BEAIOCollector(),
            "sec_edgar": SECEdgarCollector(),
            "commodity_inventory": CommodityInventoryCollector(),
            "shipping": ShippingDataCollector(),
            "critical_minerals": CriticalMineralsCollector(),
            "nfib": NFIBCollector(),
            "port_congestion": PortCongestionCollector(data_dir=self.data_dir),
            "bnpl_sec": BNPLSECCollector(),
        }
        self.logger.info("Initialized collectors", count=len(self.collectors))

    def _schedule_jobs(self) -> None:
        """Schedule all collection jobs."""
        scheduled_count = 0
        unresolved: list[str] = []

        for job_id, config in COLLECTION_SCHEDULES.items():
            collector_name = config["collector"]

            if collector_name not in self.collectors:
                unresolved.append(collector_name)
                self.logger.warning(
                    "Unknown collector",
                    job_id=job_id,
                    collector=collector_name,
                )
                continue

            if "cron" in config:
                trigger = CronTrigger.from_crontab(config["cron"])
            elif "interval_minutes" in config:
                trigger = IntervalTrigger(minutes=config["interval_minutes"])
            else:
                self.logger.warning("No schedule defined", job_id=job_id)
                continue

            self.scheduler.add_job(
                self._run_collection,
                trigger=trigger,
                id=job_id,
                name=config.get("description", job_id),
                kwargs={"collector_name": collector_name},
                replace_existing=True,
            )
            scheduled_count += 1

            self.logger.info(
                "Scheduled job",
                job_id=job_id,
                collector=collector_name,
                schedule=config.get("cron") or f"every {config.get('interval_minutes')} min",
            )

        # Log startup summary
        total = len(COLLECTION_SCHEDULES)
        self.logger.info(
            f"Scheduled {scheduled_count} of {total} collectors"
            + (f"; {len(unresolved)} unresolved: {unresolved}" if unresolved else ""),
            scheduled=scheduled_count,
            total=total,
            unresolved=unresolved,
        )

        # Schedule frequency-based retention enforcement (daily at 3 AM UTC)
        self.scheduler.add_job(
            self._enforce_retention_policy,
            trigger=CronTrigger.from_crontab("0 3 * * *"),
            id="retention_policy",
            name="Enforce frequency-based retention policy",
            replace_existing=True,
        )
        self.logger.info(
            "Scheduled retention policy enforcement",
            archive_retention_years=self.ARCHIVE_RETENTION_YEARS,
        )

        # Schedule size threshold checks for daily data (every 6 hours)
        self.scheduler.add_job(
            self._check_size_thresholds,
            trigger=CronTrigger.from_crontab("0 */6 * * *"),
            id="size_threshold_check",
            name="Check 250 GB size thresholds for daily data",
            replace_existing=True,
        )
        self.logger.info("Scheduled size threshold checks for daily data")

    async def _enforce_retention_policy(self) -> dict[str, Any]:
        """Enforce frequency-based data retention policy.

        This enforces different retention periods based on data frequency:
        - Daily data: 1 year raw retention
        - Weekly data: 2 years raw retention
        - Monthly data: 5 years raw retention
        - Quarterly/Annual data: 8 years raw retention
        - Archives: 8 years before permanent deletion

        Returns:
            Summary of cleanup operation
        """
        self.logger.info("Starting frequency-based retention policy enforcement")

        results: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "raw_retention": {},
            "archive_cleanup": {},
            "database": {},
        }

        try:
            for collector_name, frequency in COLLECTOR_FREQUENCIES.items():
                try:
                    retention_result = self.archive_manager.enforce_raw_retention(
                        source_id=collector_name,
                        frequency=frequency,
                        dry_run=False,
                    )
                    results["raw_retention"][collector_name] = retention_result
                except Exception as e:
                    self.logger.error(
                        "Raw retention enforcement failed for collector",
                        collector=collector_name,
                        error=str(e),
                    )
                    results["raw_retention"][collector_name] = {"error": str(e)}

            # Clean up old archives (8-year retention)
            archive_result = self.archive_manager.cleanup_old_archives()
            results["archive_cleanup"] = archive_result

            # Note: TimescaleDB retention is handled by the add_retention_policy
            # in init_db.sql, but we log confirmation here
            results["database"]["status"] = "managed_by_timescaledb_policy"

            total_archived = sum(
                r.get("archived_count", 0) 
                for r in results["raw_retention"].values() 
                if isinstance(r, dict)
            )
            total_bytes = sum(
                r.get("archived_bytes", 0) 
                for r in results["raw_retention"].values() 
                if isinstance(r, dict)
            )

            self.logger.info(
                "Retention policy enforced",
                raw_files_archived=total_archived,
                raw_bytes_archived=total_bytes,
                old_archives_deleted=archive_result.get("deleted_count", 0),
            )

        except Exception as e:
            self.logger.error("Retention policy enforcement failed", error=str(e))
            results["error"] = str(e)

        return results

    async def _check_size_thresholds(self) -> dict[str, Any]:
        """Check and enforce size thresholds for daily data sources.

        Daily data has a 250 GB threshold - if exceeded, oldest files are
        permanently deleted (not archived) to maintain storage limits.

        Returns:
            Summary of threshold checks
        """
        self.logger.info("Checking size thresholds for daily data sources")

        results: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "sources_checked": [],
            "sources_over_threshold": [],
        }

        daily_sources = [
            (name, freq) 
            for name, freq in COLLECTOR_FREQUENCIES.items() 
            if freq in ("daily", "real_time")
        ]

        for source_id, frequency in daily_sources:
            try:
                if self.archive_manager.check_size_threshold(source_id, frequency):
                    threshold_result = self.archive_manager.enforce_size_threshold(
                        source_id=source_id,
                        frequency=frequency,
                        dry_run=False,
                    )
                    results["sources_over_threshold"].append({
                        "source_id": source_id,
                        **threshold_result,
                    })
                else:
                    current_size = self.archive_manager.get_source_raw_size(source_id)
                    results["sources_checked"].append({
                        "source_id": source_id,
                        "current_size_gb": round(current_size / (1024**3), 2),
                        "status": "under_threshold",
                    })

            except Exception as e:
                self.logger.error(
                    "Size threshold check failed for source",
                    source_id=source_id,
                    error=str(e),
                )
                results["sources_checked"].append({
                    "source_id": source_id,
                    "error": str(e),
                })

        self.logger.info(
            "Size threshold check completed",
            sources_checked=len(results["sources_checked"]),
            sources_over_threshold=len(results["sources_over_threshold"]),
        )

        return results

    async def _run_collection(self, collector_name: str) -> CollectionResult:
        """Run a collection job.

        Args:
            collector_name: Name of the collector to run

        Returns:
            Collection result
        """
        collector = self.collectors.get(collector_name)
        if not collector:
            self.logger.error("Collector not found", collector=collector_name)
            return CollectionResult(
                collector_name=collector_name,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                success=False,
                error_message="Collector not found",
            )

        self.logger.info("Starting collection job", collector=collector_name)

        try:
            result = await collector.run_collection()

            if result.success and result.data_points:
                db = get_db()
                await db.insert_data_points(result.data_points)

                cache = get_cache()
                for dp in result.data_points[-10:]:  # Cache last 10
                    if dp.value is not None:
                        await cache.cache_series_latest(
                            dp.series_id,
                            dp.value,
                            dp.timestamp.isoformat(),
                        )

            # Store trade flow records if the collector produced any
            if result.success and hasattr(collector, "trade_flow_records") and collector.trade_flow_records:
                db = get_db()
                inserted = await db.insert_mineral_trade_flows(collector.trade_flow_records)
                self.logger.info(
                    "Stored mineral trade flows",
                    collector=collector_name,
                    count=inserted,
                )

            self.logger.info(
                "Collection job completed",
                collector=collector_name,
                success=result.success,
                records=result.records_collected,
                duration=result.duration_seconds,
            )

            return result

        except Exception as e:
            self.logger.error(
                "Collection job failed",
                collector=collector_name,
                error=str(e),
            )
            return CollectionResult(
                collector_name=collector_name,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
                success=False,
                error_message=str(e),
            )

    async def run_collector_now(self, collector_name: str) -> CollectionResult:
        """Run a collector immediately (manual trigger).

        Args:
            collector_name: Name of the collector

        Returns:
            Collection result
        """
        return await self._run_collection(collector_name)

    async def run_all_collectors(self) -> list[CollectionResult]:
        """Run every registered collector once, sequentially.

        Returns:
            List of results, one per collector.
        """
        results: list[CollectionResult] = []
        self.logger.info("Running all collectors", count=len(self.collectors))

        for name in self.collectors:
            try:
                result = await self._run_collection(name)
                results.append(result)
            except Exception as e:
                self.logger.error(
                    "Collector failed during run-all",
                    collector=name,
                    error=str(e),
                )
                results.append(
                    CollectionResult(
                        collector_name=name,
                        started_at=datetime.now(UTC),
                        completed_at=datetime.now(UTC),
                        success=False,
                        error_message=str(e),
                    )
                )

        succeeded = sum(1 for r in results if r.success)
        total_records = sum(r.records_collected for r in results)
        self.logger.info(
            "All collectors finished",
            succeeded=succeeded,
            failed=len(results) - succeeded,
            total_records=total_records,
        )
        return results

    async def start(self) -> None:
        """Start the scheduler."""
        await init_database()
        await init_cache()
        self._schedule_jobs()
        self.scheduler.start()
        self.logger.info("Scheduler started", jobs=len(self.scheduler.get_jobs()))

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.scheduler.shutdown(wait=False)
        self.logger.info("Scheduler stopped")

    def get_job_status(self) -> list[dict[str, Any]]:
        """Get status of all scheduled jobs.

        Returns:
            List of job status dictionaries
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
            })
        return jobs


# Global scheduler instance
_scheduler: DataCollectionScheduler | None = None


def get_scheduler() -> DataCollectionScheduler:
    """Get the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = DataCollectionScheduler()
    return _scheduler


async def main() -> None:
    """Main entry point for running the scheduler standalone."""
    settings = get_settings()

    from src.storage.log_handler import db_log_processor
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            db_log_processor,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Start background log flusher for the Reports tab
    from src.storage.log_handler import start_log_flusher, stop_log_flusher, flush_log_buffer
    await start_log_flusher(interval_seconds=5.0)

    scheduler = get_scheduler()

    try:
        await scheduler.start()

        logger.info("Running initial data collection...")
        for collector_name in scheduler.collectors:
            try:
                result = await scheduler.run_collector_now(collector_name)
                logger.info(
                    "Initial collection complete",
                    collector=collector_name,
                    records=result.records_collected,
                )
            except Exception as e:
                logger.error(
                    "Initial collection failed",
                    collector=collector_name,
                    error=str(e),
                )

        while True:
            await asyncio.sleep(60)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await flush_log_buffer()
        stop_log_flusher()
        await scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main())
