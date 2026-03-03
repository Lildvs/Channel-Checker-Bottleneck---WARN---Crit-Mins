"""Contrarian research detection with credibility filtering."""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from src.data_ingestion.research.base import ResearchPaper
from src.data_ingestion.research.quick_validator import (
    QuickValidator,
    TIER_1_INSTITUTIONS,
    get_validator,
)


class ResearchType(str, Enum):
    """Classification types for research papers."""

    CONSENSUS = "consensus"
    EMERGING = "emerging"
    CONTRARIAN = "contrarian"
    LOW_QUALITY_CONTRARIAN = "low_quality_contrarian"


@dataclass
class ContrarianResult:
    """Result of contrarian detection analysis."""

    research_type: ResearchType
    confidence: float  # 0-1 confidence in classification
    signals_found: list[str]  # Matched patterns/signals
    contradicts_topics: list[str]  # Topics/papers this contradicts
    is_credible: bool  # Whether author/institution is credible
    explanation: str  # Human-readable explanation


CONTRADICTION_PATTERNS = [
    (r"contrary to (?:previous|prior|earlier|established|conventional)", "contrary_to_previous"),
    (r"in contrast to (?:previous|prior|earlier|conventional|current)", "in_contrast_to"),
    (r"challenges? the (?:assumption|notion|theory|paradigm|consensus)", "challenges_assumption"),
    (r"refutes? (?:the|previous|prior|current|established)", "refutes"),
    (r"contradicts? (?:previous|prior|current|established)", "contradicts"),
    (r"overturns? (?:the|previous|prior|current)", "overturns"),
    (r"disproves? (?:the|previous|prior)", "disproves"),

    (r"(?:fails?|failure) to replicate", "replication_failure"),
    (r"replication (?:crisis|failure|problem)", "replication_crisis"),
    (r"could not (?:reproduce|replicate)", "could_not_replicate"),
    (r"not (?:reproducible|replicable)", "not_reproducible"),
    (r"irreproducible", "irreproducible"),

    (r"we show that .{0,50} is (?:incorrect|wrong|flawed|mistaken)", "shows_incorrect"),
    (r"previous findings .{0,30}(?:not supported|contradicted)", "previous_not_supported"),
    (r"questions? the validity", "questions_validity"),
    (r"calls into question", "calls_into_question"),
    (r"re-?examines? (?:the|previous|prior)", "reexamines"),
    (r"revisits? (?:the|previous|prior)", "revisits"),

    (r"alternative (?:explanation|interpretation|mechanism)", "alternative_explanation"),
    (r"competing (?:theory|hypothesis|explanation)", "competing_theory"),
    (r"differs from (?:conventional|established|previous)", "differs_from_conventional"),

    (r"negative results?", "negative_results"),
    (r"null (?:results?|findings?|effect)", "null_results"),
    (r"no (?:significant )?(?:effect|correlation|relationship)", "no_effect"),
    (r"failed to find", "failed_to_find"),
]

EMERGING_PATTERNS = [
    (r"first (?:time|ever|report)", "first_time"),
    (r"novel (?:approach|method|technique|finding)", "novel_approach"),
    (r"new (?:paradigm|framework|theory)", "new_paradigm"),
    (r"unprecedented", "unprecedented"),
    (r"breakthrough", "breakthrough"),
    (r"revolutioniz", "revolutionary"),
    (r"never (?:before|previously)", "never_before"),
    (r"first to (?:show|demonstrate|prove)", "first_to_show"),
    (r"pioneering", "pioneering"),
    (r"groundbreaking", "groundbreaking"),
]

CONSENSUS_PATTERNS = [
    (r"consistent with (?:previous|prior|earlier)", "consistent_with"),
    (r"confirms? (?:previous|prior|earlier|existing)", "confirms"),
    (r"supports? (?:the|previous|prior|existing)", "supports"),
    (r"extends? (?:the|previous|prior)", "extends"),
    (r"builds (?:on|upon)", "builds_on"),
    (r"in (?:line|agreement) with", "in_agreement"),
    (r"corroborates?", "corroborates"),
    (r"validates?", "validates"),
    (r"replicates?", "replicates"),
]


class ContrarianDetector:
    """Detects contrarian research with credibility filtering.

    Classifies papers into:
    - CONSENSUS: Builds on and supports existing research
    - EMERGING: Novel research in new areas
    - CONTRARIAN: Challenges/contradicts existing consensus with credible authors
    - LOW_QUALITY_CONTRARIAN: Contradicts but lacks credibility signals
    """

    def __init__(
        self,
        validator: QuickValidator | None = None,
        h_index_threshold: int = 10,
        min_citations_for_credibility: int = 5,
    ):
        """Initialize the detector.

        Args:
            validator: QuickValidator instance for credibility checking
            h_index_threshold: Minimum h-index for author credibility
            min_citations_for_credibility: Min citations for new paper credibility
        """
        self.validator = validator or get_validator()
        self.h_index_threshold = h_index_threshold
        self.min_citations_for_credibility = min_citations_for_credibility

        self._contradiction_patterns = [
            (re.compile(pattern, re.IGNORECASE), name)
            for pattern, name in CONTRADICTION_PATTERNS
        ]
        self._emerging_patterns = [
            (re.compile(pattern, re.IGNORECASE), name)
            for pattern, name in EMERGING_PATTERNS
        ]
        self._consensus_patterns = [
            (re.compile(pattern, re.IGNORECASE), name)
            for pattern, name in CONSENSUS_PATTERNS
        ]

    def _find_matches(
        self,
        text: str,
        patterns: list[tuple[re.Pattern, str]],
    ) -> list[str]:
        """Find all pattern matches in text."""
        matches = []
        for pattern, name in patterns:
            if pattern.search(text):
                matches.append(name)
        return matches

    def _is_credible_author(self, paper: ResearchPaper) -> bool:
        """Check if paper has credible authorship.

        Credibility is established by:
        - Author h-index > threshold
        - Author from Tier 1 institution
        - Paper has enough citations
        - Paper has code/data available
        """

        if self.validator.is_author_credible(paper, self.h_index_threshold):
            return True

        for inst in paper.institutions:
            inst_lower = inst.lower()
            for tier1 in TIER_1_INSTITUTIONS:
                if tier1 in inst_lower or inst_lower in tier1:
                    return True

        if paper.citation_count >= self.min_citations_for_credibility:
            return True

        if paper.code_url:
            return True

        return False

    def detect(self, paper: ResearchPaper) -> ContrarianResult:
        """Detect if a paper is contrarian research.

        Args:
            paper: Paper to analyze

        Returns:
            ContrarianResult with classification
        """
        text = f"{paper.title} {paper.abstract or ''}"

        contradiction_matches = self._find_matches(text, self._contradiction_patterns)
        emerging_matches = self._find_matches(text, self._emerging_patterns)
        consensus_matches = self._find_matches(text, self._consensus_patterns)

        is_credible = self._is_credible_author(paper)

        contradiction_score = len(contradiction_matches) * 0.3
        emerging_score = len(emerging_matches) * 0.25
        consensus_score = len(consensus_matches) * 0.2

        if contradiction_score >= 0.3:
            if is_credible:
                research_type = ResearchType.CONTRARIAN
                confidence = min(0.5 + contradiction_score, 0.95)
                explanation = (
                    f"Paper contains {len(contradiction_matches)} contradiction signals "
                    f"and is from credible source."
                )
            else:
                research_type = ResearchType.LOW_QUALITY_CONTRARIAN
                confidence = min(0.3 + contradiction_score * 0.5, 0.7)
                explanation = (
                    f"Paper contains {len(contradiction_matches)} contradiction signals "
                    f"but lacks credibility indicators. Flagged for review."
                )
        elif emerging_score >= 0.25 and consensus_score < 0.2:
            research_type = ResearchType.EMERGING
            confidence = min(0.4 + emerging_score, 0.85)
            explanation = (
                f"Paper shows {len(emerging_matches)} emerging/novel research signals "
                f"without heavy reliance on existing consensus."
            )
        else:
            research_type = ResearchType.CONSENSUS
            confidence = 0.5 + consensus_score
            if consensus_matches:
                explanation = (
                    f"Paper builds on existing research with {len(consensus_matches)} "
                    f"consensus-supporting signals."
                )
            else:
                explanation = "Paper appears to be standard consensus-building research."

        contradicts_topics = []
        if research_type in (ResearchType.CONTRARIAN, ResearchType.LOW_QUALITY_CONTRARIAN):
            for topic in paper.topics:
                contradicts_topics.append(topic)

        return ContrarianResult(
            research_type=research_type,
            confidence=round(confidence, 3),
            signals_found=contradiction_matches + emerging_matches + consensus_matches,
            contradicts_topics=contradicts_topics,
            is_credible=is_credible,
            explanation=explanation,
        )

    def detect_batch(
        self,
        papers: list[ResearchPaper],
    ) -> dict[str, ContrarianResult]:
        """Detect contrarian research in multiple papers.

        Args:
            papers: List of papers to analyze

        Returns:
            Dict mapping source_id to ContrarianResult
        """
        return {paper.source_id: self.detect(paper) for paper in papers}

    def update_paper_classification(
        self,
        paper: ResearchPaper,
    ) -> ResearchPaper:
        """Update a paper's research_type and contrarian fields.

        Args:
            paper: Paper to update

        Returns:
            Paper with updated fields
        """
        result = self.detect(paper)
        paper.research_type = result.research_type.value
        paper.contrarian_confidence = result.confidence
        return paper

    def filter_contrarian(
        self,
        papers: list[ResearchPaper],
        credible_only: bool = True,
    ) -> list[ResearchPaper]:
        """Filter papers to only contrarian research.

        Args:
            papers: Papers to filter
            credible_only: Whether to exclude low-quality contrarian

        Returns:
            List of contrarian papers
        """
        results = []
        for paper in papers:
            result = self.detect(paper)
            if result.research_type == ResearchType.CONTRARIAN:
                results.append(paper)
            elif (
                not credible_only
                and result.research_type == ResearchType.LOW_QUALITY_CONTRARIAN
            ):
                results.append(paper)
        return results

    def filter_emerging(
        self,
        papers: list[ResearchPaper],
    ) -> list[ResearchPaper]:
        """Filter papers to only emerging research.

        Args:
            papers: Papers to filter

        Returns:
            List of emerging research papers
        """
        results = []
        for paper in papers:
            result = self.detect(paper)
            if result.research_type == ResearchType.EMERGING:
                results.append(paper)
        return results

    def get_high_signal_papers(
        self,
        papers: list[ResearchPaper],
        min_confidence: float = 0.6,
    ) -> list[tuple[ResearchPaper, ContrarianResult]]:
        """Get papers with high contrarian/emerging signals.

        Args:
            papers: Papers to analyze
            min_confidence: Minimum confidence threshold

        Returns:
            List of (paper, result) tuples sorted by confidence
        """
        high_signal = []
        for paper in papers:
            result = self.detect(paper)
            if (
                result.research_type
                in (ResearchType.CONTRARIAN, ResearchType.EMERGING)
                and result.confidence >= min_confidence
            ):
                high_signal.append((paper, result))

        high_signal.sort(key=lambda x: x[1].confidence, reverse=True)
        return high_signal


_default_detector: ContrarianDetector | None = None


def get_detector() -> ContrarianDetector:
    """Get the default detector instance."""
    global _default_detector
    if _default_detector is None:
        _default_detector = ContrarianDetector()
    return _default_detector


def detect_contrarian(paper: ResearchPaper) -> ContrarianResult:
    """Convenience function to detect contrarian research."""
    return get_detector().detect(paper)
