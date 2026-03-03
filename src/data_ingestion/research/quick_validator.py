"""Quick validation scoring for research papers.

Speed-first validation that runs in <100ms per paper using
cached data and paper metadata.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from src.data_ingestion.research.base import ResearchPaper


@dataclass
class ValidationScore:
    """Validation score breakdown for a paper."""

    total_score: float  # 0-1 overall score
    author_score: float
    institution_score: float
    citation_score: float
    code_score: float
    reference_score: float
    age_penalty: float
    components: dict[str, float]  # Detailed breakdown


TIER_1_INSTITUTIONS = {
    "massachusetts institute of technology", "mit",
    "stanford university", "stanford",
    "university of california, berkeley", "uc berkeley", "berkeley",
    "carnegie mellon university", "cmu",
    "harvard university", "harvard",
    "california institute of technology", "caltech",
    "princeton university", "princeton",
    "yale university", "yale",
    "columbia university", "columbia",
    "university of michigan", "umich",
    "georgia institute of technology", "georgia tech",
    "university of illinois", "uiuc",
    "cornell university", "cornell",
    "university of washington", "uw",
    "university of texas at austin", "ut austin",
    "johns hopkins university", "johns hopkins",
    "university of pennsylvania", "upenn",
    "duke university", "duke",
    "northwestern university", "northwestern",
    "university of southern california", "usc",

    "university of oxford", "oxford",
    "university of cambridge", "cambridge",
    "eth zurich", "eth",
    "imperial college london", "imperial college",
    "university of toronto", "toronto",
    "tsinghua university", "tsinghua",
    "peking university", "peking",
    "university of tokyo", "tokyo",
    "technical university of munich", "tum",
    "max planck",
    "epfl",
    "national university of singapore", "nus",
    "nanyang technological university", "ntu",
    "kaist",
    "university of melbourne",
    "australian national university", "anu",

    "google research", "google deepmind", "deepmind",
    "meta ai", "facebook ai", "fair",
    "microsoft research",
    "openai",
    "ibm research",
    "nvidia research",
    "bell labs",
    "mit lincoln laboratory", "lincoln lab",
    "los alamos national laboratory", "lanl",
    "lawrence berkeley", "lbl",
    "argonne national laboratory",
    "oak ridge national laboratory", "ornl",
    "sandia national laboratories",
    "national institutes of health", "nih",
    "cern",
}

TIER_2_INSTITUTIONS = {
    "university of wisconsin", "university of maryland",
    "university of colorado", "boston university",
    "university of florida", "ohio state university",
    "penn state", "purdue university",
    "university of minnesota", "arizona state",
    "mcgill university", "university of british columbia",
    "kyoto university", "osaka university",
    "national taiwan university", "fudan university",
    "shanghai jiao tong", "zhejiang university",
    "technion", "weizmann institute",
    "kth royal institute", "delft university",
    "university of amsterdam", "lmu munich",
    "university of edinburgh", "ucl", "king's college london",
}


class QuickValidator:
    """Speed-first validation scoring for research papers.

    Uses cached author data and paper metadata to compute
    a validation score in <100ms per paper.
    """

    WEIGHTS = {
        "author_count": 0.10,
        "has_code": 0.15,
        "reference_count": 0.10,
        "citation_count": 0.15,
        "author_credibility": 0.15,
        "institution_tier": 0.15,
        "abstract_quality": 0.10,
        "age_recency": 0.10,
    }

    GOOD_AUTHOR_COUNT = (2, 6)  # Optimal author range
    GOOD_REFERENCE_COUNT = 20  # Papers with good literature review
    MIN_ABSTRACT_LENGTH = 150  # Characters

    def __init__(
        self,
        author_cache: dict[str, dict[str, Any]] | None = None,
    ):
        """Initialize the validator.

        Args:
            author_cache: Optional pre-loaded author credibility data
        """
        self.author_cache = author_cache or {}

    def _normalize_institution(self, name: str) -> str:
        """Normalize institution name for matching."""
        return name.lower().strip()

    def _get_institution_tier(self, institutions: list[str]) -> int:
        """Get the highest tier among institutions.

        Returns:
            1 for tier 1, 2 for tier 2, 3 for other
        """
        for inst in institutions:
            normalized = self._normalize_institution(inst)
            for tier1 in TIER_1_INSTITUTIONS:
                if tier1 in normalized or normalized in tier1:
                    return 1

        for inst in institutions:
            normalized = self._normalize_institution(inst)
            for tier2 in TIER_2_INSTITUTIONS:
                if tier2 in normalized or normalized in tier2:
                    return 2

        return 3

    def _score_author_count(self, count: int) -> float:
        """Score based on number of authors.

        Single-author papers get a penalty.
        2-6 authors is optimal.
        More than 6 is fine but not bonus.
        """
        if count == 0:
            return 0.0
        if count == 1:
            return 0.5  # Single author penalty
        if 2 <= count <= 6:
            return 1.0  # Optimal
        if count > 6:
            return 0.8  # Large collaborations are fine
        return 0.7

    def _score_citations(self, count: int, paper_age_days: int) -> float:
        """Score based on citations adjusted for paper age.

        Args:
            count: Citation count
            paper_age_days: Days since publication

        Returns:
            Score from 0-1
        """
        if paper_age_days <= 7:
            if count > 0:
                return 1.0
            return 0.5  # Neutral for brand new papers
        elif paper_age_days <= 30:
            if count >= 5:
                return 1.0
            if count >= 1:
                return 0.8
            return 0.5
        elif paper_age_days <= 180:
            if count >= 20:
                return 1.0
            if count >= 10:
                return 0.8
            if count >= 5:
                return 0.6
            return 0.4
        else:
            if count >= 50:
                return 1.0
            if count >= 20:
                return 0.7
            if count >= 10:
                return 0.5
            return 0.3

    def _score_references(self, count: int) -> float:
        """Score based on reference count."""
        if count >= self.GOOD_REFERENCE_COUNT:
            return 1.0
        if count >= 10:
            return 0.7
        if count >= 5:
            return 0.5
        return 0.3

    def _score_abstract(self, abstract: str | None) -> float:
        """Score based on abstract quality."""
        if not abstract:
            return 0.3
        length = len(abstract)
        if length >= self.MIN_ABSTRACT_LENGTH:
            return 1.0
        if length >= 50:
            return 0.6
        return 0.3

    def _score_age(self, published_date: datetime | None) -> float:
        """Score based on paper recency.

        More recent papers get higher scores.
        """
        if not published_date:
            return 0.5  # Unknown age, neutral

        now = datetime.now(timezone.utc)
        if published_date.tzinfo is None:
            published_date = published_date.replace(tzinfo=timezone.utc)

        age_days = (now - published_date).days

        if age_days <= 7:
            return 1.0  # Very recent
        if age_days <= 30:
            return 0.9
        if age_days <= 90:
            return 0.8
        if age_days <= 180:
            return 0.7
        if age_days <= 365:
            return 0.6
        return 0.5  # Older than a year

    def _get_author_credibility(
        self,
        authors: list[str],
    ) -> tuple[float, dict[str, Any]]:
        """Get author credibility from cache.

        Returns:
            Tuple of (score, best_author_info)
        """
        best_score = 0.0
        best_author: dict[str, Any] = {}

        for author in authors:
            author_lower = author.lower()
            for cached_id, info in self.author_cache.items():
                cached_name = info.get("name", "").lower()
                if author_lower in cached_name or cached_name in author_lower:
                    h_index = info.get("h_index", 0) or 0
                    if h_index >= 50:
                        score = 1.0
                    elif h_index >= 30:
                        score = 0.9
                    elif h_index >= 20:
                        score = 0.8
                    elif h_index >= 10:
                        score = 0.7
                    elif h_index >= 5:
                        score = 0.6
                    else:
                        score = 0.5

                    if score > best_score:
                        best_score = score
                        best_author = info

        return best_score or 0.5, best_author  # Default to 0.5 if no cache

    def validate(self, paper: ResearchPaper) -> ValidationScore:
        """Validate a paper and compute score.

        Args:
            paper: ResearchPaper to validate

        Returns:
            ValidationScore with breakdown
        """
        paper_age_days = 0
        if paper.published_date:
            now = datetime.now(timezone.utc)
            pub_date = paper.published_date
            if pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            paper_age_days = max(0, (now - pub_date).days)

        author_count_score = self._score_author_count(len(paper.authors))
        code_score = 1.0 if paper.code_url else 0.0
        reference_score = self._score_references(paper.reference_count)
        citation_score = self._score_citations(paper.citation_count, paper_age_days)
        author_cred_score, _ = self._get_author_credibility(paper.authors)

        inst_tier = self._get_institution_tier(paper.institutions)
        institution_score = {1: 1.0, 2: 0.7, 3: 0.4}[inst_tier]

        abstract_score = self._score_abstract(paper.abstract)
        age_score = self._score_age(paper.published_date)

        components = {
            "author_count": author_count_score * self.WEIGHTS["author_count"],
            "has_code": code_score * self.WEIGHTS["has_code"],
            "reference_count": reference_score * self.WEIGHTS["reference_count"],
            "citation_count": citation_score * self.WEIGHTS["citation_count"],
            "author_credibility": author_cred_score * self.WEIGHTS["author_credibility"],
            "institution_tier": institution_score * self.WEIGHTS["institution_tier"],
            "abstract_quality": abstract_score * self.WEIGHTS["abstract_quality"],
            "age_recency": age_score * self.WEIGHTS["age_recency"],
        }

        total_score = sum(components.values())

        return ValidationScore(
            total_score=round(total_score, 3),
            author_score=author_cred_score,
            institution_score=institution_score,
            citation_score=citation_score,
            code_score=code_score,
            reference_score=reference_score,
            age_penalty=1.0 - age_score,  # Inverted for clarity
            components=components,
        )

    def validate_batch(
        self,
        papers: list[ResearchPaper],
    ) -> dict[str, ValidationScore]:
        """Validate multiple papers.

        Args:
            papers: List of papers to validate

        Returns:
            Dict mapping source_id to ValidationScore
        """
        return {paper.source_id: self.validate(paper) for paper in papers}

    def update_paper_score(self, paper: ResearchPaper) -> ResearchPaper:
        """Update a paper's quick_score field.

        Args:
            paper: Paper to update

        Returns:
            Paper with updated quick_score
        """
        score = self.validate(paper)
        paper.quick_score = score.total_score
        return paper

    def is_credible(self, paper: ResearchPaper, threshold: float = 0.5) -> bool:
        """Check if a paper meets credibility threshold.

        Args:
            paper: Paper to check
            threshold: Minimum score threshold

        Returns:
            True if paper is credible
        """
        score = self.validate(paper)
        return score.total_score >= threshold

    def is_author_credible(
        self,
        paper: ResearchPaper,
        h_index_threshold: int = 10,
    ) -> bool:
        """Check if any paper author is credible.

        Args:
            paper: Paper to check
            h_index_threshold: Minimum h-index for credibility

        Returns:
            True if at least one author is credible
        """
        for author in paper.authors:
            author_lower = author.lower()
            for _, info in self.author_cache.items():
                cached_name = info.get("name", "").lower()
                if author_lower in cached_name or cached_name in author_lower:
                    h_index = info.get("h_index", 0) or 0
                    if h_index >= h_index_threshold:
                        return True

        return self._get_institution_tier(paper.institutions) == 1

    def load_author_cache(self, cache_data: dict[str, dict[str, Any]]) -> None:
        """Load author cache data.

        Args:
            cache_data: Author cache mapping author_id to info dict
        """
        self.author_cache.update(cache_data)


_default_validator: QuickValidator | None = None


def get_validator() -> QuickValidator:
    """Get the default validator instance."""
    global _default_validator
    if _default_validator is None:
        _default_validator = QuickValidator()
    return _default_validator


def validate_paper(paper: ResearchPaper) -> ValidationScore:
    """Convenience function to validate a paper."""
    return get_validator().validate(paper)
