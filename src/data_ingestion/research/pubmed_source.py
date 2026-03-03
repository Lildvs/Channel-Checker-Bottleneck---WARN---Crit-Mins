"""PubMed E-utilities API client for biomedical research papers."""

import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

from src.data_ingestion.research.base import ResearchPaper, ResearchSourceBase


class PubMedSource(ResearchSourceBase):
    """PubMed E-utilities API client.

    Provides access to biomedical and life science research.
    API docs: https://www.ncbi.nlm.nih.gov/books/NBK25500/

    Rate limit: 3 requests per second without API key
    """

    EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

    def __init__(self, api_key: str | None = None, email: str | None = None):
        """Initialize the PubMed source.

        Args:
            api_key: Optional NCBI API key for higher rate limits
            email: Email for identification (recommended by NCBI)
        """
        super().__init__(
            name="pubmed",
            base_url=self.EUTILS_BASE,
            rate_limit_delay=0.4,  # 3 req/sec = 0.33s, add buffer
            timeout=30.0,
        )
        self.api_key = api_key
        self.email = email or "research@example.com"

    def _get_headers(self) -> dict[str, str]:
        """Get headers for PubMed API."""
        return {
            "User-Agent": f"ChannelCheckResearcher/1.0 (mailto:{self.email})",
        }

    def _get_base_params(self) -> dict[str, str]:
        """Get base parameters for all requests."""
        params = {"db": "pubmed", "retmode": "xml", "tool": "channelcheck"}
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        return params

    async def _search_ids(
        self,
        query: str,
        max_results: int = 100,
        date_from: str | None = None,
    ) -> list[str]:
        """Search PubMed and return PMIDs.

        Args:
            query: Search query
            max_results: Maximum results
            date_from: Optional date filter (YYYY/MM/DD)

        Returns:
            List of PMIDs
        """
        client = await self._get_client()

        params = self._get_base_params()
        params.update({
            "term": query,
            "retmax": str(min(max_results, 200)),
            "sort": "pub_date",
        })

        if date_from:
            params["mindate"] = date_from
            params["datetype"] = "pdat"

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(f"{self.base_url}/esearch.fcgi", params=params)
            response.raise_for_status()

            root = ET.fromstring(response.text)
            id_list = root.find("IdList")
            if id_list is None:
                return []

            return [id_elem.text for id_elem in id_list.findall("Id") if id_elem.text]

        except Exception as e:
            self.logger.error("PubMed search failed", query=query, error=str(e))
            return []

    async def _fetch_articles(self, pmids: list[str]) -> list[dict[str, Any]]:
        """Fetch article details by PMIDs.

        Args:
            pmids: List of PubMed IDs

        Returns:
            List of article data dicts
        """
        if not pmids:
            return []

        client = await self._get_client()

        params = self._get_base_params()
        params["id"] = ",".join(pmids)

        try:
            await asyncio.sleep(self.rate_limit_delay)
            response = await client.get(f"{self.base_url}/efetch.fcgi", params=params)
            response.raise_for_status()

            root = ET.fromstring(response.text)
            articles = []

            for article in root.findall(".//PubmedArticle"):
                try:
                    articles.append(self._parse_article_xml(article))
                except Exception as e:
                    self.logger.warning("Failed to parse PubMed article", error=str(e))

            return articles

        except Exception as e:
            self.logger.error("Failed to fetch PubMed articles", error=str(e))
            return []

    def _parse_article_xml(self, article: ET.Element) -> dict[str, Any]:
        """Parse PubMed article XML element.

        Args:
            article: PubmedArticle XML element

        Returns:
            Parsed article data dict
        """
        medline = article.find("MedlineCitation")
        if medline is None:
            return {}

        pmid_elem = medline.find("PMID")
        pmid = pmid_elem.text if pmid_elem is not None else ""

        article_data = medline.find("Article")
        if article_data is None:
            return {"pmid": pmid}

        title_elem = article_data.find(".//ArticleTitle")
        title = self._get_text(title_elem)

        abstract_parts = []
        abstract_elem = article_data.find("Abstract")
        if abstract_elem is not None:
            for text in abstract_elem.findall("AbstractText"):
                label = text.get("Label", "")
                content = self._get_text(text)
                if label:
                    abstract_parts.append(f"{label}: {content}")
                else:
                    abstract_parts.append(content)
        abstract = " ".join(abstract_parts)

        authors = []
        institutions = []
        author_list = article_data.find("AuthorList")
        if author_list is not None:
            for author in author_list.findall("Author"):
                last_name = self._get_text(author.find("LastName"))
                fore_name = self._get_text(author.find("ForeName"))
                if last_name:
                    authors.append(f"{fore_name} {last_name}".strip())

                for affil in author.findall("AffiliationInfo/Affiliation"):
                    affil_text = self._get_text(affil)
                    if affil_text and affil_text not in institutions:
                        institutions.append(affil_text)

        pub_date = None
        journal = article_data.find("Journal")
        if journal is not None:
            pub_date_elem = journal.find(".//PubDate")
            if pub_date_elem is not None:
                year = self._get_text(pub_date_elem.find("Year"))
                month = self._get_text(pub_date_elem.find("Month")) or "01"
                day = self._get_text(pub_date_elem.find("Day")) or "01"

                month_map = {
                    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
                }
                if month in month_map:
                    month = month_map[month]

                if year:
                    try:
                        pub_date = datetime(
                            int(year),
                            int(month),
                            int(day),
                            tzinfo=timezone.utc,
                        )
                    except ValueError:
                        pub_date = datetime(int(year), 1, 1, tzinfo=timezone.utc)

        doi = None
        article_ids = article.find("PubmedData/ArticleIdList")
        if article_ids is not None:
            for id_elem in article_ids.findall("ArticleId"):
                if id_elem.get("IdType") == "doi":
                    doi = id_elem.text
                    break

        mesh_terms = []
        mesh_list = medline.find("MeshHeadingList")
        if mesh_list is not None:
            for heading in mesh_list.findall("MeshHeading"):
                descriptor = heading.find("DescriptorName")
                if descriptor is not None:
                    mesh_terms.append(descriptor.text)

        return {
            "pmid": pmid,
            "doi": doi,
            "title": title,
            "abstract": abstract,
            "authors": authors,
            "institutions": institutions,
            "published_date": pub_date,
            "mesh_terms": mesh_terms,
        }

    def _get_text(self, elem: ET.Element | None) -> str:
        """Safely get text content from an XML element."""
        if elem is None:
            return ""
        # Handle mixed content
        if elem.text:
            return elem.text.strip()
        return ""

    def _convert_to_paper(self, article: dict[str, Any]) -> ResearchPaper:
        """Convert parsed article data to ResearchPaper.

        Args:
            article: Parsed article dict

        Returns:
            ResearchPaper object
        """
        topics = set()
        mesh_topic_map = {
            "Artificial Intelligence": ["ai_ml"],
            "Machine Learning": ["ai_ml"],
            "Neural Networks, Computer": ["ai_ml"],
            "Deep Learning": ["ai_ml"],
            "Semiconductors": ["semiconductors"],
            "Energy Metabolism": ["energy"],
            "Renewable Energy": ["energy"],
            "Batteries": ["energy"],
            "Nanoparticles": ["materials"],
            "Materials Science": ["materials"],
            "Genetics": ["biotech"],
            "CRISPR-Cas Systems": ["biotech"],
            "Gene Therapy": ["biotech"],
            "Vaccines": ["biotech"],
            "Agriculture": ["agriculture"],
            "Food Supply": ["agriculture"],
            "Supply Chain": ["supply_chain"],
            "Quantum Theory": ["quantum"],
            "Telecommunications": ["telecom_connectivity"],
            "Wireless Technology": ["telecom_connectivity"],
            "Computer Security": ["cybersecurity_infosec"],
            "Cloud Computing": ["cloud_edge_compute"],
            "Robotics": ["robotics_autonomy"],
            "Exoskeleton Device": ["robotics_autonomy"],
            "Autonomous Vehicles": ["autonomous_vehicles"],
            "Aerospace Medicine": ["aerospace_defense"],
            "Blockchain": ["fintech_digital_assets"],
            "Cryptocurrency": ["fintech_digital_assets"],
            "Economics": ["macro_geoeconomics"],
            "Global Health": ["macro_geoeconomics"],
            "Printing, Three-Dimensional": ["advanced_manufacturing"],
            "Lasers": ["photonics_optics"],
            "Optics and Photonics": ["photonics_optics"],
            "Brain-Computer Interfaces": ["neurotech_bci"],
            "Neurosciences": ["neurotech_bci"],
            "Deep Brain Stimulation": ["neurotech_bci"],
            "Electroencephalography": ["neurotech_bci"],
            "Neural Prostheses": ["neurotech_bci"],
        }

        for mesh in article.get("mesh_terms", []):
            for key, topic_list in mesh_topic_map.items():
                if key.lower() in mesh.lower():
                    topics.update(topic_list)

        if not topics:
            topics.add("biotech")

        code_url = self._extract_code_url({"abstract": article.get("abstract", "")})

        pmid = article.get("pmid", "")
        return ResearchPaper(
            source_id=pmid,
            source="pubmed",
            doi=article.get("doi"),
            arxiv_id=None,
            title=article.get("title", ""),
            abstract=article.get("abstract"),
            authors=article.get("authors", []),
            institutions=article.get("institutions", []),
            published_date=article.get("published_date"),
            url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else "",
            pdf_url=None,  # PubMed doesn't always have direct PDF links
            code_url=code_url,
            topics=list(topics),
            raw_metadata={
                "pmid": pmid,
                "mesh_terms": article.get("mesh_terms", []),
            },
        )

    async def search(
        self,
        query: str,
        max_results: int = 100,
        **filters: Any,
    ) -> list[ResearchPaper]:
        """Search PubMed for articles.

        Args:
            query: Search query
            max_results: Maximum results
            **filters: Additional filters (date_from)

        Returns:
            List of ResearchPaper objects
        """
        self.logger.info("Searching PubMed", query=query, max_results=max_results)

        date_from = filters.get("date_from")
        pmids = await self._search_ids(query, max_results, date_from)

        if not pmids:
            return []

        all_papers = []
        batch_size = 50

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i : i + batch_size]
            articles = await self._fetch_articles(batch)

            for article in articles:
                try:
                    paper = self._convert_to_paper(article)
                    all_papers.append(paper)
                except Exception as e:
                    self.logger.warning("Failed to convert article", error=str(e))

        self.logger.info("PubMed search complete", results=len(all_papers))
        return all_papers

    async def fetch_recent(
        self,
        days: int = 7,
        topics: list[str] | None = None,
        max_results: int = 100,
    ) -> list[ResearchPaper]:
        """Fetch recent articles from PubMed.

        Args:
            days: Number of days to look back
            topics: Optional topics to filter by
            max_results: Maximum results

        Returns:
            List of ResearchPaper objects
        """
        from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y/%m/%d")

        topic_queries = {
            "ai_ml": "(machine learning OR artificial intelligence OR deep learning)",
            "biotech": "(biotechnology OR gene therapy OR CRISPR)",
            "materials": "(materials science OR nanotechnology)",
            "energy": "(energy storage OR battery OR renewable energy)",
            "agriculture": "(agriculture OR food security)",
            "supply_chain": "(supply chain OR logistics)",
            "quantum": "(quantum computing OR quantum mechanics)",
        }

        if topics:
            query_parts = []
            for topic in topics:
                if topic in topic_queries:
                    query_parts.append(topic_queries[topic])

            query = " OR ".join(query_parts) if query_parts else "*"
        else:
            query = "(biotechnology OR medicine OR health)"

        return await self.search(
            query=query,
            max_results=max_results,
            date_from=from_date,
        )

    async def get_paper(self, paper_id: str) -> ResearchPaper | None:
        """Get an article by PMID.

        Args:
            paper_id: PubMed ID (PMID)

        Returns:
            ResearchPaper if found, None otherwise
        """
        articles = await self._fetch_articles([paper_id])
        if articles:
            return self._convert_to_paper(articles[0])
        return None
