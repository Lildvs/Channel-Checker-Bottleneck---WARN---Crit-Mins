"""arXiv API client for fetching research papers."""

import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

from src.data_ingestion.research.base import ResearchPaper, ResearchSourceBase

ARXIV_CATEGORY_TOPICS = {
    "cond-mat.mtrl-sci": ["energy", "materials"],
    "physics.app-ph": ["energy", "semiconductors"],
    "physics.chem-ph": ["energy", "materials"],

    "cond-mat.mes-hall": ["semiconductors"],
    "cs.AR": ["semiconductors", "ai_ml"],
    "cs.ET": ["semiconductors"],

    "cs.LG": ["ai_ml"],
    "cs.AI": ["ai_ml"],
    "cs.CL": ["ai_ml"],
    "cs.CV": ["ai_ml"],
    "cs.NE": ["ai_ml", "neurotech_bci"],
    "stat.ML": ["ai_ml"],

    "quant-ph": ["quantum"],
    "cs.QC": ["quantum"],

    "q-bio": ["biotech"],
    "q-bio.BM": ["biotech"],
    "q-bio.GN": ["biotech"],
    "q-bio.NC": ["biotech", "neurotech_bci"],

    "econ.GN": ["supply_chain", "macro_geoeconomics"],
    "q-fin": ["supply_chain", "fintech_digital_assets", "macro_geoeconomics"],
    "cs.GT": ["supply_chain"],

    "cs.NI": ["telecom_connectivity", "cloud_edge_compute"],
    "cs.DC": ["cloud_edge_compute"],
    "cs.CR": ["cybersecurity_infosec"],
    "cs.IT": ["telecom_connectivity"],
    "eess.SP": ["telecom_connectivity"],

    "cs.RO": ["robotics_autonomy", "autonomous_vehicles"],
    "cs.SY": ["robotics_autonomy", "autonomous_vehicles"],
    "eess.SY": ["robotics_autonomy", "autonomous_vehicles"],
    "physics.space-ph": ["aerospace_defense"],
    "astro-ph.IM": ["aerospace_defense"],
    "astro-ph.EP": ["aerospace_defense"],

    "q-fin.CP": ["fintech_digital_assets"],
    "q-fin.TR": ["fintech_digital_assets", "macro_geoeconomics"],
    "q-fin.EC": ["macro_geoeconomics"],
    "cs.CE": ["fintech_digital_assets", "advanced_manufacturing"],

    "physics.optics": ["photonics_optics"],
    "physics.ins-det": ["photonics_optics"],
    "cond-mat.soft": ["materials", "advanced_manufacturing"],
    "q-bio.TO": ["neurotech_bci"],
}


class ArxivSource(ResearchSourceBase):
    """arXiv API client for fetching preprints.

    Uses the arXiv API (https://arxiv.org/help/api)
    Rate limit: 1 request per 3 seconds
    """

    ARXIV_API_URL = "http://export.arxiv.org/api/query"

    def __init__(self):
        """Initialize the arXiv source."""
        super().__init__(
            name="arxiv",
            base_url=self.ARXIV_API_URL,
            rate_limit_delay=3.0,  # arXiv requires 3 seconds between requests
            timeout=60.0,
        )

    def _get_headers(self) -> dict[str, str]:
        """Get headers for arXiv API."""
        return {
            "User-Agent": "ChannelCheckResearcher/1.0 (Economic Research Tool)",
        }

    def _build_query(
        self,
        search_query: str | None = None,
        categories: list[str] | None = None,
        date_from: datetime | None = None,
    ) -> str:
        """Build arXiv search query string.

        Args:
            search_query: Free text search
            categories: arXiv category codes
            date_from: Only papers after this date

        Returns:
            URL-encoded query string
        """
        parts = []

        if search_query:
            parts.append(f"all:{search_query}")

        if categories:
            cat_query = " OR ".join(f"cat:{cat}" for cat in categories)
            parts.append(f"({cat_query})")

        query = " AND ".join(parts) if parts else "all:*"
        return quote(query)

    def _parse_entry(self, entry: ET.Element, ns: dict[str, str]) -> ResearchPaper:
        """Parse an arXiv API entry into a ResearchPaper.

        Args:
            entry: XML entry element
            ns: Namespace dict

        Returns:
            ResearchPaper object
        """
        id_elem = entry.find("atom:id", ns)
        arxiv_url = id_elem.text if id_elem is not None else ""
        arxiv_id = arxiv_url.split("/abs/")[-1] if arxiv_url else ""

        doi = None
        for link in entry.findall("atom:link", ns):
            if link.get("title") == "doi":
                doi = link.get("href", "").replace("http://dx.doi.org/", "")

        title_elem = entry.find("atom:title", ns)
        title = title_elem.text.strip().replace("\n", " ") if title_elem is not None else ""

        summary_elem = entry.find("atom:summary", ns)
        abstract = summary_elem.text.strip() if summary_elem is not None else None

        authors = []
        for author in entry.findall("atom:author", ns):
            name_elem = author.find("atom:name", ns)
            if name_elem is not None:
                authors.append(name_elem.text)

        institutions = []
        for author in entry.findall("atom:author", ns):
            for affil in author.findall("arxiv:affiliation", ns):
                if affil.text and affil.text not in institutions:
                    institutions.append(affil.text)

        published_elem = entry.find("atom:published", ns)
        published_date = self._parse_date(
            published_elem.text if published_elem is not None else None
        )

        pdf_url = None
        for link in entry.findall("atom:link", ns):
            if link.get("type") == "application/pdf":
                pdf_url = link.get("href")
                break

        topics = set()
        for category in entry.findall("arxiv:primary_category", ns):
            cat = category.get("term", "")
            if cat in ARXIV_CATEGORY_TOPICS:
                topics.update(ARXIV_CATEGORY_TOPICS[cat])
        for category in entry.findall("atom:category", ns):
            cat = category.get("term", "")
            if cat in ARXIV_CATEGORY_TOPICS:
                topics.update(ARXIV_CATEGORY_TOPICS[cat])

        code_url = self._extract_code_url({"abstract": abstract})

        return ResearchPaper(
            source_id=arxiv_id,
            source="arxiv",
            doi=doi,
            arxiv_id=arxiv_id,
            title=title,
            abstract=abstract,
            authors=authors,
            institutions=institutions,
            published_date=published_date,
            url=arxiv_url,
            pdf_url=pdf_url,
            code_url=code_url,
            topics=list(topics),
            raw_metadata={
                "categories": [
                    cat.get("term", "")
                    for cat in entry.findall("atom:category", ns)
                ],
            },
        )

    async def search(
        self,
        query: str,
        max_results: int = 100,
        **filters: Any,
    ) -> list[ResearchPaper]:
        """Search arXiv for papers matching query.

        Args:
            query: Search query string
            max_results: Maximum results to return
            **filters: Additional filters (categories, date_from)

        Returns:
            List of ResearchPaper objects
        """
        client = await self._get_client()

        categories = filters.get("categories")
        date_from = filters.get("date_from")

        search_query = self._build_query(query, categories, date_from)

        params = {
            "search_query": search_query,
            "start": 0,
            "max_results": min(max_results, 200),  # arXiv caps at 200
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        self.logger.info(
            "Searching arXiv",
            query=query,
            max_results=max_results,
        )

        try:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()

            root = ET.fromstring(response.text)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "arxiv": "http://arxiv.org/schemas/atom",
            }

            papers = []
            for entry in root.findall("atom:entry", ns):
                try:
                    paper = self._parse_entry(entry, ns)
                    papers.append(paper)
                except Exception as e:
                    self.logger.warning("Failed to parse arXiv entry", error=str(e))
                    continue

            self.logger.info("arXiv search complete", results=len(papers))
            return papers

        except Exception as e:
            self.logger.error("arXiv search failed", error=str(e))
            return []

    async def fetch_recent(
        self,
        days: int = 7,
        topics: list[str] | None = None,
        max_results: int = 100,
    ) -> list[ResearchPaper]:
        """Fetch recent papers from arXiv.

        Args:
            days: Number of days to look back
            topics: Optional topics to filter by
            max_results: Maximum results

        Returns:
            List of ResearchPaper objects
        """
        categories = []
        if topics:
            for cat, cat_topics in ARXIV_CATEGORY_TOPICS.items():
                if any(t in cat_topics for t in topics):
                    categories.append(cat)
        else:
            categories = list(ARXIV_CATEGORY_TOPICS.keys())

        # arXiv API doesn't support date filtering directly,
        # so we fetch more and filter client-side
        all_papers = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        category_groups = [
            ["cs.LG", "cs.AI", "cs.CL", "cs.CV"],  # AI/ML
            ["cond-mat.mtrl-sci", "cond-mat.mes-hall", "physics.app-ph"],  # Materials/Physics
            ["quant-ph"],  # Quantum
            ["q-bio", "q-bio.BM", "q-bio.GN"],  # Biotech
            ["econ.GN", "q-fin"],  # Economics
        ]

        for cat_group in category_groups:
            if categories and not any(c in categories for c in cat_group):
                continue

            await asyncio.sleep(self.rate_limit_delay)

            papers = await self.search(
                query="",
                max_results=min(max_results, 50),
                categories=cat_group,
            )

            for paper in papers:
                if paper.published_date and paper.published_date >= cutoff_date:
                    all_papers.append(paper)

            if len(all_papers) >= max_results:
                break

        return all_papers[:max_results]

    async def get_paper(self, paper_id: str) -> ResearchPaper | None:
        """Get a specific paper by arXiv ID.

        Args:
            paper_id: arXiv ID (e.g., "2301.00001")

        Returns:
            ResearchPaper if found, None otherwise
        """
        client = await self._get_client()

        params = {
            "id_list": paper_id,
        }

        try:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()

            root = ET.fromstring(response.text)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "arxiv": "http://arxiv.org/schemas/atom",
            }

            entries = root.findall("atom:entry", ns)
            if entries:
                return self._parse_entry(entries[0], ns)

            return None

        except Exception as e:
            self.logger.error("Failed to get arXiv paper", paper_id=paper_id, error=str(e))
            return None
