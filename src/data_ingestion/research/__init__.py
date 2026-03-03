"""Research intelligence module for academic paper collection and analysis."""

from src.data_ingestion.research.base import ResearchPaper, ResearchSourceBase
from src.data_ingestion.research.arxiv_source import ArxivSource
from src.data_ingestion.research.semantic_scholar_source import SemanticScholarSource
from src.data_ingestion.research.openal_source import OpenAlexSource
from src.data_ingestion.research.pubmed_source import PubMedSource
from src.data_ingestion.research.topic_classifier import TopicClassifier, classify_paper
from src.data_ingestion.research.quick_validator import QuickValidator, validate_paper
from src.data_ingestion.research.contrarian_detector import (
    ContrarianDetector,
    ContrarianResult,
    ResearchType,
    detect_contrarian,
)
from src.data_ingestion.research.aggregator import ResearchAggregator, AggregationStats

__all__ = [
    "ResearchPaper",
    "ResearchSourceBase",
    "ArxivSource",
    "SemanticScholarSource",
    "OpenAlexSource",
    "PubMedSource",
    "TopicClassifier",
    "classify_paper",
    "QuickValidator",
    "validate_paper",
    "ContrarianDetector",
    "ContrarianResult",
    "ResearchType",
    "detect_contrarian",
    "ResearchAggregator",
    "AggregationStats",
]
