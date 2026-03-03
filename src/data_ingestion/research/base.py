"""Base class for research paper sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

logger = structlog.get_logger()


@dataclass
class ResearchPaper:
    """Represents a research paper from any source."""

    source_id: str  # ID within the source (arxiv id, semantic scholar id, etc.)
    source: str  # arxiv, semantic_scholar, openal, pubmed
    doi: str | None = None
    arxiv_id: str | None = None

    title: str = ""
    abstract: str | None = None
    authors: list[str] = field(default_factory=list)
    institutions: list[str] = field(default_factory=list)
    published_date: datetime | None = None

    url: str = ""
    pdf_url: str | None = None
    code_url: str | None = None

    citation_count: int = 0
    reference_count: int = 0

    topics: list[str] = field(default_factory=list)
    quick_score: float = 0.5
    research_type: str = "consensus"  # consensus, emerging, contrarian
    contrarian_confidence: float = 0.0
    contradicts_papers: list[str] = field(default_factory=list)

    collected_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    raw_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "doi": self.doi,
            "arxiv_id": self.arxiv_id,
            "title": self.title,
            "abstract": self.abstract,
            "authors": self.authors,
            "institutions": self.institutions,
            "published_date": self.published_date,
            "source": self.source,
            "topics": self.topics,
            "quick_score": self.quick_score,
            "citation_count": self.citation_count,
            "reference_count": self.reference_count,
            "research_type": self.research_type,
            "contrarian_confidence": self.contrarian_confidence,
            "contradicts_papers": self.contradicts_papers,
            "collected_at": self.collected_at,
            "url": self.url,
            "pdf_url": self.pdf_url,
            "code_url": self.code_url,
            "raw_metadata": self.raw_metadata,
        }


class ResearchSourceBase(ABC):
    """Abstract base class for research paper sources.

    Provides a common interface for fetching papers from various sources
    like arXiv, Semantic Scholar, OpenAlex, and PubMed.
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        rate_limit_delay: float = 1.0,
        timeout: float = 30.0,
    ):
        """Initialize the research source.

        Args:
            name: Source name (e.g., 'arxiv', 'semantic_scholar')
            base_url: Base URL for the API
            rate_limit_delay: Seconds to wait between requests
            timeout: HTTP request timeout in seconds
        """
        self.name = name
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self.logger = logger.bind(source=name)

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
                headers=self._get_headers(),
            )
        return self._client

    def _get_headers(self) -> dict[str, str]:
        """Get default HTTP headers. Override in subclasses for custom headers."""
        return {
            "User-Agent": "ChannelCheckResearcher/1.0 (Economic Research Tool; mailto:research@example.com)",
            "Accept": "application/json",
        }

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @abstractmethod
    async def search(
        self,
        query: str,
        max_results: int = 100,
        **filters: Any,
    ) -> list[ResearchPaper]:
        """Search for papers by query.

        Args:
            query: Search query string
            max_results: Maximum number of results to return
            **filters: Additional source-specific filters

        Returns:
            List of ResearchPaper objects
        """
        ...

    @abstractmethod
    async def fetch_recent(
        self,
        days: int = 7,
        topics: list[str] | None = None,
        max_results: int = 100,
    ) -> list[ResearchPaper]:
        """Fetch recently published papers.

        Args:
            days: Number of days to look back
            topics: Optional list of topics to filter by
            max_results: Maximum number of results

        Returns:
            List of ResearchPaper objects
        """
        ...

    @abstractmethod
    async def get_paper(self, paper_id: str) -> ResearchPaper | None:
        """Get a single paper by its ID.

        Args:
            paper_id: Source-specific paper identifier

        Returns:
            ResearchPaper if found, None otherwise
        """
        ...

    async def get_citations(self, paper_id: str) -> int:
        """Get citation count for a paper.

        Default implementation returns 0. Override in sources that support this.

        Args:
            paper_id: Source-specific paper identifier

        Returns:
            Citation count
        """
        return 0

    async def get_author_info(self, author_id: str) -> dict[str, Any] | None:
        """Get author information.

        Default implementation returns None. Override in sources that support this.

        Args:
            author_id: Source-specific author identifier

        Returns:
            Author info dict or None
        """
        return None

    def _parse_date(self, date_str: str | None) -> datetime | None:
        """Parse a date string into datetime.

        Args:
            date_str: Date string in various formats

        Returns:
            Parsed datetime or None
        """
        if not date_str:
            return None

        import re
        from datetime import timezone

        formats = [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y%m%d",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        year_match = re.search(r"(\d{4})", date_str)
        if year_match:
            return datetime(int(year_match.group(1)), 1, 1, tzinfo=timezone.utc)

        return None

    def _extract_code_url(self, paper: dict[str, Any]) -> str | None:
        """Extract code/data repository URL from paper metadata.

        Args:
            paper: Raw paper metadata

        Returns:
            URL to code/data repository or None
        """
        import re

        text = str(paper.get("abstract", "")) + str(paper.get("body", ""))

        github_pattern = r"https?://github\.com/[\w-]+/[\w.-]+"
        match = re.search(github_pattern, text)
        if match:
            return match.group(0)

        gitlab_pattern = r"https?://gitlab\.com/[\w-]+/[\w.-]+"
        match = re.search(gitlab_pattern, text)
        if match:
            return match.group(0)

        zenodo_pattern = r"https?://zenodo\.org/record/\d+"
        match = re.search(zenodo_pattern, text)
        if match:
            return match.group(0)

        return None
