"""OpenAlex API client for fetching research papers."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from src.data_ingestion.research.base import ResearchPaper, ResearchSourceBase


class OpenAlexSource(ResearchSourceBase):
    """OpenAlex API client.

    OpenAlex is a fully open catalog of 250M+ scholarly works.
    API docs: https://docs.openalex.org/

    Rate limit: Polite pool (include email in User-Agent)
    """

    BASE_URL = "https://api.openalex.org"

    CONCEPT_TOPICS = {
        "C41008148": ["ai_ml", "cloud_edge_compute", "cybersecurity_infosec"],  # Computer Science
        "C119857082": ["ai_ml"],  # Machine Learning
        "C154945302": ["ai_ml"],  # Artificial Intelligence
        "C108827166": ["ai_ml"],  # Deep Learning

        "C127413603": ["energy"],  # Energy
        "C62520636": ["energy"],  # Renewable Energy
        "C178790620": ["energy"],  # Battery

        "C192562407": ["materials", "advanced_manufacturing"],  # Materials Science
        "C159985019": ["materials", "advanced_manufacturing"],  # Nanotechnology

        "C185592680": ["semiconductors", "photonics_optics"],  # Semiconductor

        "C86803240": ["biotech"],  # Biology
        "C71924100": ["biotech", "neurotech_bci"],  # Medicine
        "C54355233": ["biotech"],  # Genetics

        "C118552586": ["agriculture"],  # Agricultural Science
        "C166957645": ["agriculture"],  # Food Science

        "C162324750": ["supply_chain", "macro_geoeconomics"],  # Economics
        "C144133560": ["supply_chain", "fintech_digital_assets"],  # Business

        "C62520637": ["quantum"],  # Quantum Mechanics

        "C31258907": ["telecom_connectivity"],  # Telecommunications
        "C199360897": ["cloud_edge_compute"],  # Cloud Computing
        "C38652104": ["cybersecurity_infosec"],  # Computer Security

        "C90856448": ["robotics_autonomy"],  # Robotics
        "C121332964": ["aerospace_defense"],  # Aerospace Engineering
        "C39549134": ["autonomous_vehicles"],  # Autonomous Vehicle

        "C17744445": ["fintech_digital_assets"],  # Cryptocurrency
        "C10138342": ["macro_geoeconomics"],  # Macroeconomics
        "C149923435": ["fintech_digital_assets"],  # Blockchain

        "C120665830": ["advanced_manufacturing"],  # 3D Printing
        "C120314980": ["photonics_optics"],  # Optics
        "C15744967": ["neurotech_bci"],  # Neuroscience
        "C126838900": ["neurotech_bci"],  # Brain-Computer Interface
    }

    def __init__(self, email: str | None = None):
        """Initialize the OpenAlex source.

        Args:
            email: Email for polite pool access (recommended)
        """
        super().__init__(
            name="openal",
            base_url=self.BASE_URL,
            rate_limit_delay=0.5,  # OpenAlex is generous
            timeout=30.0,
        )
        self.email = email or "research@example.com"

    def _get_headers(self) -> dict[str, str]:
        """Get headers for OpenAlex API."""
        return {
            "User-Agent": f"ChannelCheckResearcher/1.0 (mailto:{self.email})",
            "Accept": "application/json",
        }

    def _parse_paper(self, data: dict[str, Any]) -> ResearchPaper:
        """Parse OpenAlex work response.

        Args:
            data: Work data from API

        Returns:
            ResearchPaper object
        """
        doi = None
        arxiv_id = None
        ids = data.get("ids") or {}
        if ids.get("doi"):
            doi = ids["doi"].replace("https://doi.org/", "")
        if ids.get("arxiv"):
            arxiv_id = ids["arxiv"].replace("https://arxiv.org/abs/", "")

        authors = []
        institutions = []
        for authorship in data.get("authorships") or []:
            author = authorship.get("author") or {}
            if author.get("display_name"):
                authors.append(author["display_name"])

            for inst in authorship.get("institutions") or []:
                if inst.get("display_name") and inst["display_name"] not in institutions:
                    institutions.append(inst["display_name"])

        pub_date_str = data.get("publication_date")
        published_date = self._parse_date(pub_date_str)

        topics = set()
        for concept in data.get("concepts") or []:
            concept_id = concept.get("id", "").split("/")[-1]
            if concept_id in self.CONCEPT_TOPICS:
                topics.update(self.CONCEPT_TOPICS[concept_id])

        url = data.get("id") or ""
        pdf_url = None
        oa = data.get("open_access") or {}
        if oa.get("oa_url"):
            pdf_url = oa["oa_url"]

        abstract_text = ""
        abstract_inverted = data.get("abstract_inverted_index") or {}
        if abstract_inverted:
            word_positions = []
            for word, positions in abstract_inverted.items():
                for pos in positions:
                    word_positions.append((pos, word))
            word_positions.sort()
            abstract_text = " ".join(word for _, word in word_positions)

        code_url = self._extract_code_url({"abstract": abstract_text})

        return ResearchPaper(
            source_id=data.get("id", "").split("/")[-1],
            source="openal",
            doi=doi,
            arxiv_id=arxiv_id,
            title=data.get("title") or "",
            abstract=abstract_text if abstract_text else None,
            authors=authors,
            institutions=institutions,
            published_date=published_date,
            url=url,
            pdf_url=pdf_url,
            code_url=code_url,
            citation_count=data.get("cited_by_count", 0) or 0,
            reference_count=len(data.get("referenced_works") or []),
            topics=list(topics),
            raw_metadata={
                "openalex_id": data.get("id"),
                "concepts": [c.get("display_name") for c in data.get("concepts", [])],
                "type": data.get("type"),
            },
        )

    async def search(
        self,
        query: str,
        max_results: int = 100,
        **filters: Any,
    ) -> list[ResearchPaper]:
        """Search OpenAlex for works.

        Args:
            query: Search query
            max_results: Maximum results
            **filters: Additional filters (from_date, concepts)

        Returns:
            List of ResearchPaper objects
        """
        client = await self._get_client()

        papers = []
        cursor = "*"
        per_page = min(200, max_results)  # OpenAlex allows up to 200

        while len(papers) < max_results and cursor:
            params = {
                "search": query,
                "per_page": per_page,
                "cursor": cursor,
                "sort": "publication_date:desc",
            }

            filter_parts = []
            if filters.get("from_date"):
                filter_parts.append(f"from_publication_date:{filters['from_date']}")
            if filters.get("concepts"):
                concepts = "|".join(filters["concepts"])
                filter_parts.append(f"concepts.id:{concepts}")
            if filter_parts:
                params["filter"] = ",".join(filter_parts)

            self.logger.info(
                "Searching OpenAlex",
                query=query,
                cursor=cursor[:20] if cursor != "*" else "*",
            )

            try:
                await asyncio.sleep(self.rate_limit_delay)
                response = await client.get(
                    f"{self.base_url}/works",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                results = data.get("results") or []
                if not results:
                    break

                for work_data in results:
                    try:
                        paper = self._parse_paper(work_data)
                        papers.append(paper)
                    except Exception as e:
                        self.logger.warning(
                            "Failed to parse OpenAlex work",
                            error=str(e),
                        )

                meta = data.get("meta") or {}
                cursor = meta.get("next_cursor")
                if not cursor:
                    break

            except Exception as e:
                self.logger.error("OpenAlex search failed", error=str(e))
                break

        self.logger.info("OpenAlex search complete", results=len(papers))
        return papers[:max_results]

    async def fetch_recent(
        self,
        days: int = 7,
        topics: list[str] | None = None,
        max_results: int = 100,
    ) -> list[ResearchPaper]:
        """Fetch recent works from OpenAlex.

        Args:
            days: Number of days to look back
            topics: Optional topics to filter by
            max_results: Maximum results

        Returns:
            List of ResearchPaper objects
        """
        from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

        concept_ids = []
        if topics:
            topic_concept_map = {
                "ai_ml": ["C41008148", "C119857082", "C154945302"],
                "semiconductors": ["C185592680"],
                "energy": ["C127413603", "C62520636"],
                "materials": ["C192562407", "C159985019"],
                "biotech": ["C86803240", "C71924100"],
                "agriculture": ["C118552586", "C166957645"],
                "supply_chain": ["C162324750", "C144133560"],
                "quantum": ["C62520636"],
            }
            for topic in topics:
                if topic in topic_concept_map:
                    concept_ids.extend(topic_concept_map[topic])
            concept_ids = list(set(concept_ids))

        return await self.search(
            query="*",  # All works
            max_results=max_results,
            from_date=from_date,
            concepts=concept_ids if concept_ids else None,
        )

    async def get_paper(self, paper_id: str) -> ResearchPaper | None:
        """Get a work by OpenAlex ID or DOI.

        Args:
            paper_id: OpenAlex work ID or DOI

        Returns:
            ResearchPaper if found, None otherwise
        """
        client = await self._get_client()

        if paper_id.startswith("10."):
            url = f"{self.base_url}/works/https://doi.org/{paper_id}"
        elif paper_id.startswith("W"):
            url = f"{self.base_url}/works/{paper_id}"
        else:
            url = f"{self.base_url}/works/{paper_id}"

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

            return self._parse_paper(data)

        except Exception as e:
            self.logger.error(
                "Failed to get OpenAlex work",
                paper_id=paper_id,
                error=str(e),
            )
            return None

    async def get_citations(self, paper_id: str) -> int:
        """Get citation count for a work.

        Args:
            paper_id: OpenAlex work ID

        Returns:
            Citation count
        """
        paper = await self.get_paper(paper_id)
        return paper.citation_count if paper else 0

    async def get_author_info(self, author_id: str) -> dict[str, Any] | None:
        """Get author information from OpenAlex.

        Args:
            author_id: OpenAlex author ID

        Returns:
            Author info dict
        """
        client = await self._get_client()

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(f"{self.base_url}/authors/{author_id}")
            response.raise_for_status()
            data = response.json()

            affiliations = []
            for affil in data.get("affiliations") or []:
                inst = affil.get("institution") or {}
                if inst.get("display_name"):
                    affiliations.append(inst["display_name"])

            return {
                "author_id": data.get("id"),
                "name": data.get("display_name"),
                "h_index": data.get("summary_stats", {}).get("h_index"),
                "total_citations": data.get("cited_by_count", 0),
                "paper_count": data.get("works_count", 0),
                "affiliations": affiliations,
            }

        except Exception as e:
            self.logger.error(
                "Failed to get author info",
                author_id=author_id,
                error=str(e),
            )
            return None
