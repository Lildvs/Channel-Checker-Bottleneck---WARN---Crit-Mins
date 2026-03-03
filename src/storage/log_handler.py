"""Database log handler that persists structlog entries to the system_logs table.

Captures WARNING, ERROR, and CRITICAL log events so they are visible
in the GUI Reports tab. Uses a background queue to avoid blocking
the logging call path.
"""

import asyncio
import threading
from collections import deque
from datetime import UTC, datetime
from typing import Any

import structlog

from src.storage.models import SystemLog

logger = structlog.get_logger(__name__)

# Minimum levels to persist (in-memory filter before DB write)
_PERSIST_LEVELS = frozenset({"warning", "error", "critical"})

# In-memory buffer for log entries awaiting flush
_log_buffer: deque[dict[str, Any]] = deque(maxlen=5000)
_buffer_lock = threading.Lock()

# Sentinel for whether the async flusher is running
_flusher_running = False


def db_log_processor(
    _logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Structlog processor that queues WARNING+ events for DB persistence.

    This is added to the structlog processor chain. It does NOT alter the
    event_dict -- it only enqueues a copy for the background flusher.
    """
    if method_name in _PERSIST_LEVELS:
        entry = {
            "timestamp": event_dict.get("timestamp", datetime.now(UTC).isoformat()),
            "level": method_name.upper(),
            "logger_name": event_dict.get("logger", ""),
            "event": str(event_dict.get("event", "")),
            "source_module": event_dict.get("module", event_dict.get("logger", "")),
            "extra_data": {
                k: _safe_serialize(v)
                for k, v in event_dict.items()
                if k not in ("event", "timestamp", "level", "logger", "module")
            },
        }
        with _buffer_lock:
            _log_buffer.append(entry)

    return event_dict


def _safe_serialize(value: Any) -> Any:
    """Make a value JSON-safe for storage in JSONB."""
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_safe_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _safe_serialize(v) for k, v in value.items()}
    return str(value)


async def flush_log_buffer() -> int:
    """Flush buffered log entries to the database.

    Returns the number of entries flushed.
    """
    with _buffer_lock:
        if not _log_buffer:
            return 0
        entries = list(_log_buffer)
        _log_buffer.clear()

    try:
        from src.storage.timescale import get_db

        db = get_db()
        async with db.session() as session:
            for entry in entries:
                log_entry = SystemLog(
                    timestamp=datetime.fromisoformat(entry["timestamp"])
                    if isinstance(entry["timestamp"], str)
                    else entry["timestamp"],
                    level=entry["level"],
                    logger_name=entry.get("logger_name"),
                    event=entry["event"],
                    source_module=entry.get("source_module"),
                    extra_data=entry.get("extra_data", {}),
                )
                session.add(log_entry)
            await session.commit()
        return len(entries)
    except Exception as exc:
        # Put entries back on failure (best-effort, may lose some on overflow)
        with _buffer_lock:
            _log_buffer.extendleft(reversed(entries))
        # Use print to avoid recursive logging
        print(f"[log_handler] Failed to flush {len(entries)} log entries: {exc}")
        return 0


async def start_log_flusher(interval_seconds: float = 5.0) -> None:
    """Start the periodic background flusher.

    Should be called once at application startup (e.g., in lifespan).
    """
    global _flusher_running
    if _flusher_running:
        return
    _flusher_running = True

    async def _loop() -> None:
        global _flusher_running
        while _flusher_running:
            await asyncio.sleep(interval_seconds)
            try:
                await flush_log_buffer()
            except Exception:
                pass  # Swallow -- print already happened inside flush

    asyncio.create_task(_loop())


def stop_log_flusher() -> None:
    """Signal the background flusher to stop."""
    global _flusher_running
    _flusher_running = False
