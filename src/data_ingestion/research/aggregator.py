"""Multi-source research aggregator with deduplication."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog

from src.data_ingestion.research.base import ResearchPaper, ResearchSourceBase
from src.data_ingestion.research.arxiv_source import ArxivSource
from src.data_ingestion.research.semantic_scholar_source import SemanticScholarSource
from src.data_ingestion.research.openal_source import OpenAlexSource
from src.data_ingestion.research.pubmed_source import PubMedSource
from src.data_ingestion.research.topic_classifier import TopicClassifier, get_classifier
from src.data_ingestion.research.quick_validator import QuickValidator, get_validator
from src.data_ingestion.research.contrarian_detector import (
    ContrarianDetector,
    get_detector,
)

logger = structlog.get_logger()


@dataclass
class AggregationStats:
    """Statistics from an aggregation run."""

    total_fetched: int
    after_dedup: int
    by_source: dict[str, int]
    by_topic: dict[str, int]
    by_type: dict[str, int]  # consensus, emerging, contrarian
    processing_time_ms: int


class ResearchAggregator:
    """Aggregates research from multiple sources with deduplication.

    Combines papers from arXiv, Semantic Scholar, OpenAlex, and PubMed,
    deduplicates by DOI/arXiv ID/title, and enriches with topic classification,
    validation scoring, and contrarian detection.
    """

    def __init__(
        self,
        sources: list[ResearchSourceBase] | None = None,
        classifier: TopicClassifier | None = None,
        validator: QuickValidator | None = None,
        detector: ContrarianDetector | None = None,
        title_similarity_threshold: float = 0.9,
    ):
        """Initialize the aggregator.

        Args:
            sources: List of research sources to use
            classifier: Topic classifier instance
            validator: Validation scoring instance
            detector: Contrarian detector instance
            title_similarity_threshold: Threshold for fuzzy title matching
        """
        if sources is None:
            self.sources: list[ResearchSourceBase] = [
                ArxivSource(),
                SemanticScholarSource(),
                OpenAlexSource(),
                PubMedSource(),
            ]
        else:
            self.sources = sources

        self.classifier = classifier or get_classifier()
        self.validator = validator or get_validator()
        self.detector = detector or get_detector()
        self.title_threshold = title_similarity_threshold

        self.logger = logger.bind(component="research_aggregator")

    async def close(self) -> None:
        """Close all source connections."""
        for source in self.sources:
            await source.close()

    def _normalize_title(self, title: str) -> str:
        """Normalize title for comparison."""
        import re

        normalized = title.lower()
        normalized = re.sub(r"[^\w\s]", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _title_similarity(self, title1: str, title2: str) -> float:
        """Calculate title similarity using fuzzy matching.

        Uses rapidfuzz for speed, falls back to simple ratio if unavailable.
        """
        try:
            from rapidfuzz import fuzz

            return fuzz.ratio(
                self._normalize_title(title1),
                self._normalize_title(title2),
            ) / 100.0
        except ImportError:
            # Fallback to simple substring check
            t1 = self._normalize_title(title1)
            t2 = self._normalize_title(title2)
            if t1 == t2:
                return 1.0
            if t1 in t2 or t2 in t1:
                return 0.9
            words1 = set(t1.split())
            words2 = set(t2.split())
            if not words1 or not words2:
                return 0.0
            return len(words1 & words2) / len(words1 | words2)

    def _deduplicate(self, papers: list[ResearchPaper]) -> list[ResearchPaper]:
        """Deduplicate papers by DOI, arXiv ID, or title similarity.

        When duplicates are found, prefer:
        1. Papers with more metadata (citations, abstracts)
        2. Papers from sources with better citation data
        """
        seen_dois: dict[str, ResearchPaper] = {}
        seen_arxiv: dict[str, ResearchPaper] = {}
        seen_titles: dict[str, ResearchPaper] = {}
        unique_papers: list[ResearchPaper] = []

        source_priority = {
            "semantic_scholar": 4,  # Best for citations
            "openal": 3,
            "pubmed": 2,
            "arxiv": 1,  # Good for preprints
        }

        for paper in papers:
            is_duplicate = False
            existing_paper = None

            if paper.doi:
                doi_lower = paper.doi.lower()
                if doi_lower in seen_dois:
                    is_duplicate = True
                    existing_paper = seen_dois[doi_lower]

            if not is_duplicate and paper.arxiv_id:
                arxiv_lower = paper.arxiv_id.lower()
                if arxiv_lower in seen_arxiv:
                    is_duplicate = True
                    existing_paper = seen_arxiv[arxiv_lower]

            # Check title similarity (more expensive)
            if not is_duplicate and paper.title:
                normalized = self._normalize_title(paper.title)
                for seen_title, seen_paper in seen_titles.items():
                    if self._title_similarity(normalized, seen_title) >= self.title_threshold:
                        is_duplicate = True
                        existing_paper = seen_paper
                        break

            if is_duplicate and existing_paper:
                existing_priority = source_priority.get(existing_paper.source, 0)
                new_priority = source_priority.get(paper.source, 0)

                existing_score = (
                    existing_priority * 10
                    + (1 if existing_paper.abstract else 0)
                    + (1 if existing_paper.citation_count > 0 else 0)
                    + (1 if existing_paper.code_url else 0)
                )
                new_score = (
                    new_priority * 10
                    + (1 if paper.abstract else 0)
                    + (1 if paper.citation_count > 0 else 0)
                    + (1 if paper.code_url else 0)
                )

                if new_score > existing_score:
                    if existing_paper in unique_papers:
                        unique_papers.remove(existing_paper)
                    unique_papers.append(paper)
                    if paper.doi:
                        seen_dois[paper.doi.lower()] = paper
                    if paper.arxiv_id:
                        seen_arxiv[paper.arxiv_id.lower()] = paper
                    if paper.title:
                        seen_titles[self._normalize_title(paper.title)] = paper

            else:
                unique_papers.append(paper)
                if paper.doi:
                    seen_dois[paper.doi.lower()] = paper
                if paper.arxiv_id:
                    seen_arxiv[paper.arxiv_id.lower()] = paper
                if paper.title:
                    seen_titles[self._normalize_title(paper.title)] = paper

        return unique_papers

    def _enrich_paper(self, paper: ResearchPaper) -> ResearchPaper:
        """Enrich a paper with topic classification, validation, and contrarian detection."""
        paper = self.classifier.update_paper_topics(paper)
        paper = self.validator.update_paper_score(paper)
        paper = self.detector.update_paper_classification(paper)

        return paper

    async def collect_recent(
        self,
        days: int = 7,
        topics: list[str] | None = None,
        max_per_source: int = 100,
        enrich: bool = True,
    ) -> tuple[list[ResearchPaper], AggregationStats]:
        """Collect recent papers from all sources.

        Args:
            days: Number of days to look back
            topics: Optional topics to filter by
            max_per_source: Maximum papers per source
            enrich: Whether to run enrichment pipeline

        Returns:
            Tuple of (papers, stats)
        """
        start_time = datetime.now(timezone.utc)

        self.logger.info(
            "Starting research collection",
            days=days,
            topics=topics,
            max_per_source=max_per_source,
        )

        fetch_tasks = []
        for source in self.sources:
            fetch_tasks.append(
                source.fetch_recent(
                    days=days,
                    topics=topics,
                    max_results=max_per_source,
                )
            )

        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        all_papers: list[ResearchPaper] = []
        by_source: dict[str, int] = {}

        for source, result in zip(self.sources, results):
            if isinstance(result, Exception):
                self.logger.error(
                    "Source fetch failed",
                    source=source.name,
                    error=str(result),
                )
                by_source[source.name] = 0
            else:
                all_papers.extend(result)
                by_source[source.name] = len(result)
                self.logger.info(
                    "Source fetch complete",
                    source=source.name,
                    papers=len(result),
                )

        total_fetched = len(all_papers)

        unique_papers = self._deduplicate(all_papers)
        self.logger.info(
            "Deduplication complete",
            before=total_fetched,
            after=len(unique_papers),
        )

        if enrich:
            unique_papers = [self._enrich_paper(p) for p in unique_papers]

        by_topic: dict[str, int] = {}
        by_type: dict[str, int] = {}

        for paper in unique_papers:
            for topic in paper.topics:
                by_topic[topic] = by_topic.get(topic, 0) + 1
            by_type[paper.research_type] = by_type.get(paper.research_type, 0) + 1

        elapsed_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

        stats = AggregationStats(
            total_fetched=total_fetched,
            after_dedup=len(unique_papers),
            by_source=by_source,
            by_topic=by_topic,
            by_type=by_type,
            processing_time_ms=elapsed_ms,
        )

        self.logger.info(
            "Research collection complete",
            total_papers=len(unique_papers),
            processing_time_ms=elapsed_ms,
        )

        return unique_papers, stats

    async def search_all_sources(
        self,
        query: str,
        max_per_source: int = 50,
        enrich: bool = True,
    ) -> tuple[list[ResearchPaper], AggregationStats]:
        """Search all sources for papers matching query.

        Args:
            query: Search query
            max_per_source: Max results per source
            enrich: Whether to run enrichment pipeline

        Returns:
            Tuple of (papers, stats)
        """
        start_time = datetime.now(timezone.utc)

        self.logger.info("Starting research search", query=query)

        search_tasks = []
        for source in self.sources:
            search_tasks.append(
                source.search(query=query, max_results=max_per_source)
            )

        results = await asyncio.gather(*search_tasks, return_exceptions=True)

        all_papers: list[ResearchPaper] = []
        by_source: dict[str, int] = {}

        for source, result in zip(self.sources, results):
            if isinstance(result, Exception):
                self.logger.error(
                    "Source search failed",
                    source=source.name,
                    error=str(result),
                )
                by_source[source.name] = 0
            else:
                all_papers.extend(result)
                by_source[source.name] = len(result)

        total_fetched = len(all_papers)

        unique_papers = self._deduplicate(all_papers)
        if enrich:
            unique_papers = [self._enrich_paper(p) for p in unique_papers]

        by_topic: dict[str, int] = {}
        by_type: dict[str, int] = {}

        for paper in unique_papers:
            for topic in paper.topics:
                by_topic[topic] = by_topic.get(topic, 0) + 1
            by_type[paper.research_type] = by_type.get(paper.research_type, 0) + 1

        elapsed_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

        stats = AggregationStats(
            total_fetched=total_fetched,
            after_dedup=len(unique_papers),
            by_source=by_source,
            by_topic=by_topic,
            by_type=by_type,
            processing_time_ms=elapsed_ms,
        )

        return unique_papers, stats

    async def get_paper_by_doi(self, doi: str) -> ResearchPaper | None:
        """Get a specific paper by DOI from any source.

        Args:
            doi: Digital Object Identifier

        Returns:
            ResearchPaper if found
        """
        for source in self.sources:
            if source.name == "semantic_scholar":
                paper = await source.get_paper(doi)
                if paper:
                    return self._enrich_paper(paper)

        for source in self.sources:
            if source.name != "semantic_scholar":
                try:
                    paper = await source.get_paper(doi)
                    if paper:
                        return self._enrich_paper(paper)
                except Exception:
                    continue

        return None

    def filter_by_topic(
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
        return [p for p in papers if any(t in p.topics for t in topics)]

    def filter_contrarian(
        self,
        papers: list[ResearchPaper],
        credible_only: bool = True,
    ) -> list[ResearchPaper]:
        """Filter to only contrarian papers.

        Args:
            papers: Papers to filter
            credible_only: Exclude low-quality contrarian

        Returns:
            Contrarian papers
        """
        return self.detector.filter_contrarian(papers, credible_only)

    def filter_emerging(
        self,
        papers: list[ResearchPaper],
    ) -> list[ResearchPaper]:
        """Filter to only emerging research.

        Args:
            papers: Papers to filter

        Returns:
            Emerging research papers
        """
        return self.detector.filter_emerging(papers)

    def sort_by_score(
        self,
        papers: list[ResearchPaper],
        descending: bool = True,
    ) -> list[ResearchPaper]:
        """Sort papers by validation score.

        Args:
            papers: Papers to sort
            descending: Sort highest first

        Returns:
            Sorted papers
        """
        return sorted(papers, key=lambda p: p.quick_score, reverse=descending)


_default_aggregator: ResearchAggregator | None = None


def get_aggregator() -> ResearchAggregator:
    """Get the default aggregator instance."""
    global _default_aggregator
    if _default_aggregator is None:
        _default_aggregator = ResearchAggregator()
    return _default_aggregator
