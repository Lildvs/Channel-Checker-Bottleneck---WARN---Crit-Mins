"""WARN Act layoff notice collector package.

Government-first scraping with layoffdata.com fallback and cross-validation.
"""

from src.data_ingestion.collectors.warn.collector import WARNCollector
from src.data_ingestion.collectors.warn.models import WARNRecord, StateWARNConfig

__all__ = ["WARNCollector", "WARNRecord", "StateWARNConfig"]
