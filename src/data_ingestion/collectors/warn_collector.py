"""Backwards-compatible re-export of the WARN collector package.

All functionality has moved to src.data_ingestion.collectors.warn/.
This file exists solely to avoid breaking existing imports.
"""

from src.data_ingestion.collectors.warn.collector import WARNCollector
from src.data_ingestion.collectors.warn.models import StateWARNConfig, WARNRecord
from src.data_ingestion.collectors.warn.state_configs import STATE_CONFIGS, STATE_NAME_TO_CODE

__all__ = [
    "WARNCollector",
    "WARNRecord",
    "StateWARNConfig",
    "STATE_CONFIGS",
    "STATE_NAME_TO_CODE",
]
