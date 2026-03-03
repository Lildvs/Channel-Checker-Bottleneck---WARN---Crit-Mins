"""Semantic Scholar API client for fetching papers and author data."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from src.data_ingestion.research.base import ResearchPaper, ResearchSourceBase


class SemanticScholarSource(ResearchSourceBase):
    """Semantic Scholar API client.

    Provides access to:
    - Paper metadata, abstracts, citations
    - Author information (h-index, citations)
    - Paper recommendations

    Rate limit: 100 requests per 5 minutes (free tier)
    API docs: https://api.semanticscholar.org/api-docs/
    """

    BASE_URL = "https://api.semanticscholar.org/graph/v1"

    PAPER_FIELDS = [
        "paperId",
        "externalIds",
        "title",
        "abstract",
        "year",
        "publicationDate",
        "authors",
        "citationCount",
        "referenceCount",
        "url",
        "openAccessPdf",
        "fieldsOfStudy",
    ]

    AUTHOR_FIELDS = [
        "authorId",
        "name",
        "affiliations",
        "hIndex",
        "citationCount",
        "paperCount",
    ]

    def __init__(self, api_key: str | None = None):
        """Initialize the Semantic Scholar source.

        Args:
            api_key: Optional API key for higher rate limits
        """
        super().__init__(
            name="semantic_scholar",
            base_url=self.BASE_URL,
            rate_limit_delay=3.0,  # Conservative for free tier
            timeout=30.0,
        )
        self.api_key = api_key

    def _get_headers(self) -> dict[str, str]:
        """Get headers for Semantic Scholar API."""
        headers = {
            "User-Agent": "ChannelCheckResearcher/1.0 (Economic Research Tool)",
        }
        if self.api_key:
            headers["x-api-key"] = self.api_key
        return headers

    def _parse_paper(self, data: dict[str, Any]) -> ResearchPaper:
        """Parse Semantic Scholar paper response.

        Args:
            data: Paper data from API

        Returns:
            ResearchPaper object
        """
        external_ids = data.get("externalIds") or {}
        doi = external_ids.get("DOI")
        arxiv_id = external_ids.get("ArXiv")

        authors = []
        institutions = []
        for author in data.get("authors") or []:
            if author.get("name"):
                authors.append(author["name"])
            # Semantic Scholar doesn't always include affiliations in paper response
            for affil in author.get("affiliations") or []:
                if affil and affil not in institutions:
                    institutions.append(affil)

        pub_date_str = data.get("publicationDate")
        if pub_date_str:
            published_date = self._parse_date(pub_date_str)
        elif data.get("year"):
            published_date = datetime(data["year"], 1, 1, tzinfo=timezone.utc)
        else:
            published_date = None

        pdf_data = data.get("openAccessPdf") or {}
        pdf_url = pdf_data.get("url")

        topics = []
        fields = data.get("fieldsOfStudy") or []
        field_topic_map = {
            "Computer Science": ["ai_ml", "semiconductors", "cloud_edge_compute", "cybersecurity_infosec"],
            "Physics": ["energy", "materials", "quantum", "photonics_optics"],
            "Materials Science": ["materials", "advanced_manufacturing"],
            "Chemistry": ["materials", "energy"],
            "Biology": ["biotech", "neurotech_bci"],
            "Medicine": ["biotech", "neurotech_bci"],
            "Economics": ["supply_chain", "macro_geoeconomics", "fintech_digital_assets"],
            "Business": ["supply_chain", "macro_geoeconomics"],
            "Engineering": ["semiconductors", "energy", "robotics_autonomy", "advanced_manufacturing"],
            "Environmental Science": ["agriculture", "energy"],
            "Agricultural and Food Sciences": ["agriculture"],
            "Psychology": ["neurotech_bci"],
            "Sociology": ["macro_geoeconomics"],
            "Political Science": ["macro_geoeconomics"],
        }
        for field in fields:
            if field in field_topic_map:
                topics.extend(field_topic_map[field])
        topics = list(set(topics))

        code_url = self._extract_code_url({"abstract": data.get("abstract", "")})

        return ResearchPaper(
            source_id=data.get("paperId", ""),
            source="semantic_scholar",
            doi=doi,
            arxiv_id=arxiv_id,
            title=data.get("title", ""),
            abstract=data.get("abstract"),
            authors=authors,
            institutions=institutions,
            published_date=published_date,
            url=data.get("url", ""),
            pdf_url=pdf_url,
            code_url=code_url,
            citation_count=data.get("citationCount", 0) or 0,
            reference_count=data.get("referenceCount", 0) or 0,
            topics=topics,
            raw_metadata={
                "paperId": data.get("paperId"),
                "fieldsOfStudy": fields,
            },
        )

    async def search(
        self,
        query: str,
        max_results: int = 100,
        **filters: Any,
    ) -> list[ResearchPaper]:
        """Search Semantic Scholar for papers.

        Args:
            query: Search query
            max_results: Maximum results
            **filters: Additional filters (year, fields_of_study)

        Returns:
            List of ResearchPaper objects
        """
        client = await self._get_client()

        papers = []
        offset = 0
        limit = min(100, max_results)  # API max is 100 per request

        while len(papers) < max_results:
            params = {
                "query": query,
                "offset": offset,
                "limit": limit,
                "fields": ",".join(self.PAPER_FIELDS),
            }

            if filters.get("year"):
                params["year"] = filters["year"]
            if filters.get("fields_of_study"):
                params["fieldsOfStudy"] = ",".join(filters["fields_of_study"])

            self.logger.info(
                "Searching Semantic Scholar",
                query=query,
                offset=offset,
            )

            try:
                await asyncio.sleep(self.rate_limit_delay)
                response = await client.get(
                    f"{self.base_url}/paper/search",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                results = data.get("data") or []
                if not results:
                    break

                for paper_data in results:
                    try:
                        paper = self._parse_paper(paper_data)
                        papers.append(paper)
                    except Exception as e:
                        self.logger.warning(
                            "Failed to parse Semantic Scholar paper",
                            error=str(e),
                        )

                offset += len(results)

                if len(results) < limit:
                    break

            except Exception as e:
                self.logger.error("Semantic Scholar search failed", error=str(e))
                break

        self.logger.info("Semantic Scholar search complete", results=len(papers))
        return papers[:max_results]

    async def fetch_recent(
        self,
        days: int = 7,
        topics: list[str] | None = None,
        max_results: int = 100,
    ) -> list[ResearchPaper]:
        """Fetch recent papers from Semantic Scholar.

        Args:
            days: Number of days to look back
            topics: Optional topics to filter by
            max_results: Maximum results

        Returns:
            List of ResearchPaper objects
        """
        fields_of_study = []
        if topics:
            topic_field_map = {
                "ai_ml": ["Computer Science"],
                "semiconductors": ["Computer Science", "Engineering"],
                "energy": ["Physics", "Chemistry", "Engineering"],
                "materials": ["Materials Science", "Physics", "Chemistry"],
                "biotech": ["Biology", "Medicine"],
                "agriculture": ["Agricultural and Food Sciences", "Environmental Science"],
                "supply_chain": ["Economics", "Business"],
                "quantum": ["Physics"],
            }
            for topic in topics:
                if topic in topic_field_map:
                    fields_of_study.extend(topic_field_map[topic])
            fields_of_study = list(set(fields_of_study))

        current_year = datetime.now().year

        queries = [
            "machine learning",
            "deep learning",
            "energy storage",
            "semiconductor",
            "materials science",
            "biotechnology",
            "supply chain",
        ]

        all_papers = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        for query in queries[:3]:  # Limit queries to avoid rate limiting
            await asyncio.sleep(self.rate_limit_delay)

            papers = await self.search(
                query=query,
                max_results=min(50, max_results),
                year=f"{current_year - 1}-{current_year}",
                fields_of_study=fields_of_study if fields_of_study else None,
            )

            for paper in papers:
                if paper.published_date and paper.published_date >= cutoff_date:
                    is_dup = any(
                        (p.doi and p.doi == paper.doi)
                        or (p.title.lower() == paper.title.lower())
                        for p in all_papers
                    )
                    if not is_dup:
                        all_papers.append(paper)

            if len(all_papers) >= max_results:
                break

        return all_papers[:max_results]

    async def get_paper(self, paper_id: str) -> ResearchPaper | None:
        """Get a paper by Semantic Scholar ID or DOI.

        Args:
            paper_id: Semantic Scholar paper ID or DOI

        Returns:
            ResearchPaper if found, None otherwise
        """
        client = await self._get_client()

        if "/" in paper_id:
            paper_id = f"DOI:{paper_id}"

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(
                f"{self.base_url}/paper/{paper_id}",
                params={"fields": ",".join(self.PAPER_FIELDS)},
            )
            response.raise_for_status()
            data = response.json()

            return self._parse_paper(data)

        except Exception as e:
            self.logger.error(
                "Failed to get Semantic Scholar paper",
                paper_id=paper_id,
                error=str(e),
            )
            return None

    async def get_citations(self, paper_id: str) -> int:
        """Get citation count for a paper.

        Args:
            paper_id: Semantic Scholar paper ID

        Returns:
            Citation count
        """
        paper = await self.get_paper(paper_id)
        return paper.citation_count if paper else 0

    async def get_author_info(self, author_id: str) -> dict[str, Any] | None:
        """Get author information from Semantic Scholar.

        Args:
            author_id: Semantic Scholar author ID

        Returns:
            Author info dict with h_index, citations, etc.
        """
        client = await self._get_client()

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(
                f"{self.base_url}/author/{author_id}",
                params={"fields": ",".join(self.AUTHOR_FIELDS)},
            )
            response.raise_for_status()
            data = response.json()

            return {
                "author_id": data.get("authorId"),
                "name": data.get("name"),
                "h_index": data.get("hIndex"),
                "total_citations": data.get("citationCount", 0),
                "paper_count": data.get("paperCount", 0),
                "affiliations": data.get("affiliations", []),
            }

        except Exception as e:
            self.logger.error(
                "Failed to get author info",
                author_id=author_id,
                error=str(e),
            )
            return None

    async def search_authors(
        self,
        query: str,
        max_results: int = 10,
    ) -> list[dict[str, Any]]:
        """Search for authors.

        Args:
            query: Author name to search
            max_results: Maximum results

        Returns:
            List of author info dicts
        """
        client = await self._get_client()

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(
                f"{self.base_url}/author/search",
                params={
                    "query": query,
                    "limit": min(max_results, 100),
                    "fields": ",".join(self.AUTHOR_FIELDS),
                },
            )
            response.raise_for_status()
            data = response.json()

            authors = []
            for author_data in data.get("data") or []:
                authors.append({
                    "author_id": author_data.get("authorId"),
                    "name": author_data.get("name"),
                    "h_index": author_data.get("hIndex"),
                    "total_citations": author_data.get("citationCount", 0),
                    "paper_count": author_data.get("paperCount", 0),
                    "affiliations": author_data.get("affiliations", []),
                })

            return authors

        except Exception as e:
            self.logger.error("Author search failed", query=query, error=str(e))
            return []
