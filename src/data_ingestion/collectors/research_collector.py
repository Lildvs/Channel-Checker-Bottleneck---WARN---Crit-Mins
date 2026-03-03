"""Research Intelligence Collector for academic papers.

Collects research papers from multiple sources (arXiv, Semantic Scholar,
OpenAlex, PubMed), applies topic classification, validation scoring,
and contrarian detection.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

from src.data_ingestion.base_collector import BaseCollector, CollectionResult, DataFrequency
from src.data_ingestion.research.aggregator import ResearchAggregator, AggregationStats
from src.data_ingestion.research.base import ResearchPaper

logger = structlog.get_logger()


@dataclass
class ResearchCollectionResult:
    """Result of a research collection run."""

    success: bool
    papers: list[ResearchPaper]
    stats: AggregationStats | None
    error: str | None = None
    collection_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ResearchCollector(BaseCollector):
    """Collector for academic research papers.

    Integrates with the data ingestion scheduler to periodically
    collect research papers from multiple sources.
    """

    def __init__(
        self,
        lookback_days: int = 7,
        max_per_source: int = 100,
        topics: list[str] | None = None,
    ):
        """Initialize the research collector.

        Args:
            lookback_days: Number of days to look back for papers
            max_per_source: Maximum papers to fetch per source
            topics: Optional list of topics to focus on
        """
        super().__init__(
            name="Research Intelligence Collector",
            source_id="research",
        )
        self.lookback_days = lookback_days
        self.max_per_source = max_per_source
        self.topics = topics
        self._aggregator: ResearchAggregator | None = None
        self.logger = logger.bind(collector="research")

    @property
    def aggregator(self) -> ResearchAggregator:
        """Lazy-load the aggregator."""
        if self._aggregator is None:
            self._aggregator = ResearchAggregator()
        return self._aggregator

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list:
        """Collect research papers.

        Note: series_ids, start_date, end_date are ignored for research collection.
        The collector uses lookback_days instead.

        Returns empty list because research papers are stored in a separate
        table (research_papers), not as DataPoints. The actual collection
        and metadata are handled by run_collection().

        Returns:
            Empty list (papers stored separately)
        """
        return []

    async def run_collection(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> CollectionResult:
        """Run a full research collection cycle.

        Overrides BaseCollector.run_collection() because research papers
        are stored in a separate table, not as DataPoints.
        """
        started_at = datetime.now(timezone.utc)
        self.logger.info(
            "Starting research collection",
            lookback_days=self.lookback_days,
            topics=self.topics,
        )

        try:
            papers, stats = await self.aggregator.collect_recent(
                days=self.lookback_days,
                topics=self.topics,
                max_per_source=self.max_per_source,
                enrich=True,
            )

            completed_at = datetime.now(timezone.utc)

            self.logger.info(
                "Research collection complete",
                total_papers=len(papers),
                by_source=stats.by_source,
                by_topic=stats.by_topic,
                duration_seconds=(completed_at - started_at).total_seconds(),
            )

            return CollectionResult(
                collector_name=self.name,
                started_at=started_at,
                completed_at=completed_at,
                success=True,
                data_points=[],  # Papers stored separately
                records_collected=len(papers),
                metadata={
                    "papers_collected": len(papers),
                    "stats": {
                        "total_fetched": stats.total_fetched,
                        "after_dedup": stats.after_dedup,
                        "by_source": stats.by_source,
                        "by_topic": stats.by_topic,
                        "by_type": stats.by_type,
                        "processing_time_ms": stats.processing_time_ms,
                    },
                    "papers": [p.to_dict() for p in papers],
                },
            )

        except Exception as e:
            self.logger.error("Research collection failed", error=str(e))
            return CollectionResult(
                collector_name=self.name,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                success=False,
                error_message=str(e),
            )

    async def collect_research(
        self,
        topics: list[str] | None = None,
        days: int | None = None,
    ) -> ResearchCollectionResult:
        """Direct collection method returning papers.

        Args:
            topics: Override topics for this collection
            days: Override lookback days

        Returns:
            ResearchCollectionResult with papers
        """
        use_topics = topics if topics is not None else self.topics
        use_days = days if days is not None else self.lookback_days

        try:
            papers, stats = await self.aggregator.collect_recent(
                days=use_days,
                topics=use_topics,
                max_per_source=self.max_per_source,
                enrich=True,
            )

            return ResearchCollectionResult(
                success=True,
                papers=papers,
                stats=stats,
            )

        except Exception as e:
            self.logger.error("Research collection failed", error=str(e))
            return ResearchCollectionResult(
                success=False,
                papers=[],
                stats=None,
                error=str(e),
            )

    async def search(
        self,
        query: str,
        max_results: int = 100,
    ) -> ResearchCollectionResult:
        """Search for papers across all sources.

        Args:
            query: Search query
            max_results: Maximum results per source

        Returns:
            ResearchCollectionResult with papers
        """
        try:
            papers, stats = await self.aggregator.search_all_sources(
                query=query,
                max_per_source=max_results,
                enrich=True,
            )

            return ResearchCollectionResult(
                success=True,
                papers=papers,
                stats=stats,
            )

        except Exception as e:
            self.logger.error("Research search failed", query=query, error=str(e))
            return ResearchCollectionResult(
                success=False,
                papers=[],
                stats=None,
                error=str(e),
            )

    def get_contrarian_papers(
        self,
        papers: list[ResearchPaper],
        credible_only: bool = True,
    ) -> list[ResearchPaper]:
        """Filter papers to contrarian research only.

        Args:
            papers: Papers to filter
            credible_only: Exclude low-quality contrarian

        Returns:
            Contrarian papers
        """
        return self.aggregator.filter_contrarian(papers, credible_only)

    def get_emerging_papers(
        self,
        papers: list[ResearchPaper],
    ) -> list[ResearchPaper]:
        """Filter papers to emerging research only.

        Args:
            papers: Papers to filter

        Returns:
            Emerging research papers
        """
        return self.aggregator.filter_emerging(papers)

    def get_papers_by_topic(
        self,
        papers: list[ResearchPaper],
        topics: list[str],
    ) -> list[ResearchPaper]:
        """Filter papers by topics.

        Args:
            papers: Papers to filter
            topics: Topics to include

        Returns:
            Filtered papers
        """
        return self.aggregator.filter_by_topic(papers, topics)

    async def validate_api_key(self) -> bool:
        """Research collector uses free APIs, no key validation needed."""
        return True

    def get_default_series(self) -> list[str]:
        """Research collector doesn't use series IDs."""
        return []

    def get_schedule(self) -> str:
        """Return cron schedule for research collection.

        Runs every 6 hours.
        """
        return "0 */6 * * *"

    @property
    def frequency(self) -> DataFrequency:
        """Research is collected every 6 hours."""
        return DataFrequency.DAILY

    async def close(self) -> None:
        """Close aggregator connections."""
        if self._aggregator:
            await self._aggregator.close()


async def collect_research(
    topics: list[str] | None = None,
    days: int = 7,
) -> ResearchCollectionResult:
    """Convenience function to collect research papers.

    Args:
        topics: Optional topics to filter
        days: Lookback days

    Returns:
        ResearchCollectionResult
    """
    collector = ResearchCollector(lookback_days=days, topics=topics)
    try:
        return await collector.collect_research()
    finally:
        await collector.close()
