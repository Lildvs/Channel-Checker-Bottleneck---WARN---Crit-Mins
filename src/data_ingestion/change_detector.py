"""Change detection for download-only data sources."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
import structlog

logger = structlog.get_logger()


class ChangeDetectionMethod(str, Enum):
    """Methods for detecting data changes."""

    ETAG = "etag"
    LAST_MODIFIED = "last_modified"
    CONTENT_HASH = "content_hash"
    PUBLICATION_DATE = "publication_date"
    RSS_FEED = "rss_feed"


@dataclass
class ChangeDetectionResult:
    """Result of a change detection check."""

    url: str
    has_changed: bool
    detection_method: ChangeDetectionMethod
    current_value: str | None = None
    previous_value: str | None = None
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    error_message: str | None = None


@dataclass
class DataSourceState:
    """Persisted state for a data source."""

    source_id: str
    url: str
    etag: str | None = None
    last_modified: str | None = None
    content_hash: str | None = None
    publication_date: str | None = None
    last_checked: datetime | None = None
    last_changed: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class ChangeDetector:
    """Detects changes in download-only data sources.

    Supports multiple detection methods:
    - HTTP ETag/Last-Modified headers
    - Content hashing (SHA-256)
    - Publication date parsing from landing pages
    - RSS/Atom feed monitoring
    """

    def __init__(
        self,
        state_dir: Path | None = None,
        timeout: float = 30.0,
    ):
        """Initialize the change detector.

        Args:
            state_dir: Directory to store state files
            timeout: HTTP request timeout in seconds
        """
        self.state_dir = state_dir or Path("data/manifests")
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self.logger = logger.bind(component="ChangeDetector")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
                headers={
                    "User-Agent": "ChannelCheckResearcher/1.0 (Economic Research Tool)",
                },
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _load_state(self, source_id: str) -> DataSourceState | None:
        """Load persisted state for a source."""
        state_file = self.state_dir / f"{source_id}_state.json"
        if not state_file.exists():
            return None

        try:
            data = json.loads(state_file.read_text())
            return DataSourceState(
                source_id=data["source_id"],
                url=data["url"],
                etag=data.get("etag"),
                last_modified=data.get("last_modified"),
                content_hash=data.get("content_hash"),
                publication_date=data.get("publication_date"),
                last_checked=(
                    datetime.fromisoformat(data["last_checked"])
                    if data.get("last_checked")
                    else None
                ),
                last_changed=(
                    datetime.fromisoformat(data["last_changed"])
                    if data.get("last_changed")
                    else None
                ),
                metadata=data.get("metadata", {}),
            )
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.warning(
                "Failed to load state",
                source_id=source_id,
                error=str(e),
            )
            return None

    def _save_state(self, state: DataSourceState) -> None:
        """Save state for a source."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        state_file = self.state_dir / f"{state.source_id}_state.json"

        data = {
            "source_id": state.source_id,
            "url": state.url,
            "etag": state.etag,
            "last_modified": state.last_modified,
            "content_hash": state.content_hash,
            "publication_date": state.publication_date,
            "last_checked": state.last_checked.isoformat() if state.last_checked else None,
            "last_changed": state.last_changed.isoformat() if state.last_changed else None,
            "metadata": state.metadata,
        }

        state_file.write_text(json.dumps(data, indent=2))
        self.logger.debug("State saved", source_id=state.source_id)

    async def check_etag(
        self,
        source_id: str,
        url: str,
    ) -> ChangeDetectionResult:
        """Check for changes using HTTP ETag header.

        Args:
            source_id: Identifier for the source
            url: URL to check

        Returns:
            ChangeDetectionResult with has_changed status
        """
        client = await self._get_client()
        state = self._load_state(source_id)

        try:
            response = await client.head(url)
            response.raise_for_status()

            current_etag = response.headers.get("etag")
            if not current_etag:
                return ChangeDetectionResult(
                    url=url,
                    has_changed=True,  # Can't determine, assume changed
                    detection_method=ChangeDetectionMethod.ETAG,
                    error_message="No ETag header in response",
                )

            previous_etag = state.etag if state else None
            has_changed = previous_etag is None or current_etag != previous_etag

            if state is None:
                state = DataSourceState(source_id=source_id, url=url)
            state.etag = current_etag
            state.last_checked = datetime.now(UTC)
            if has_changed:
                state.last_changed = datetime.now(UTC)
            self._save_state(state)

            return ChangeDetectionResult(
                url=url,
                has_changed=has_changed,
                detection_method=ChangeDetectionMethod.ETAG,
                current_value=current_etag,
                previous_value=previous_etag,
            )

        except httpx.RequestError as e:
            self.logger.error(
                "ETag check failed",
                url=url,
                error=str(e),
            )
            return ChangeDetectionResult(
                url=url,
                has_changed=True,  # Assume changed on error
                detection_method=ChangeDetectionMethod.ETAG,
                error_message=str(e),
            )

    async def check_last_modified(
        self,
        source_id: str,
        url: str,
    ) -> ChangeDetectionResult:
        """Check for changes using HTTP Last-Modified header.

        Args:
            source_id: Identifier for the source
            url: URL to check

        Returns:
            ChangeDetectionResult with has_changed status
        """
        client = await self._get_client()
        state = self._load_state(source_id)

        try:
            response = await client.head(url)
            response.raise_for_status()

            current_modified = response.headers.get("last-modified")
            if not current_modified:
                return ChangeDetectionResult(
                    url=url,
                    has_changed=True,
                    detection_method=ChangeDetectionMethod.LAST_MODIFIED,
                    error_message="No Last-Modified header in response",
                )

            previous_modified = state.last_modified if state else None
            has_changed = previous_modified is None or current_modified != previous_modified

            if state is None:
                state = DataSourceState(source_id=source_id, url=url)
            state.last_modified = current_modified
            state.last_checked = datetime.now(UTC)
            if has_changed:
                state.last_changed = datetime.now(UTC)
            self._save_state(state)

            return ChangeDetectionResult(
                url=url,
                has_changed=has_changed,
                detection_method=ChangeDetectionMethod.LAST_MODIFIED,
                current_value=current_modified,
                previous_value=previous_modified,
            )

        except httpx.RequestError as e:
            self.logger.error(
                "Last-Modified check failed",
                url=url,
                error=str(e),
            )
            return ChangeDetectionResult(
                url=url,
                has_changed=True,
                detection_method=ChangeDetectionMethod.LAST_MODIFIED,
                error_message=str(e),
            )

    async def check_content_hash(
        self,
        source_id: str,
        url: str,
        sample_bytes: int | None = None,
    ) -> ChangeDetectionResult:
        """Check for changes by hashing file content.

        Args:
            source_id: Identifier for the source
            url: URL to check
            sample_bytes: If set, only hash first N bytes (for large files)

        Returns:
            ChangeDetectionResult with has_changed status
        """
        client = await self._get_client()
        state = self._load_state(source_id)

        try:
            if sample_bytes:
                # Use range request for large files
                headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
                response = await client.get(url, headers=headers)
            else:
                response = await client.get(url)
            response.raise_for_status()

            content_hash = hashlib.sha256(response.content).hexdigest()
            previous_hash = state.content_hash if state else None
            has_changed = previous_hash is None or content_hash != previous_hash

            if state is None:
                state = DataSourceState(source_id=source_id, url=url)
            state.content_hash = content_hash
            state.last_checked = datetime.now(UTC)
            if has_changed:
                state.last_changed = datetime.now(UTC)
            self._save_state(state)

            return ChangeDetectionResult(
                url=url,
                has_changed=has_changed,
                detection_method=ChangeDetectionMethod.CONTENT_HASH,
                current_value=content_hash[:16] + "...",
                previous_value=previous_hash[:16] + "..." if previous_hash else None,
            )

        except httpx.RequestError as e:
            self.logger.error(
                "Content hash check failed",
                url=url,
                error=str(e),
            )
            return ChangeDetectionResult(
                url=url,
                has_changed=True,
                detection_method=ChangeDetectionMethod.CONTENT_HASH,
                error_message=str(e),
            )

    async def check_publication_date(
        self,
        source_id: str,
        url: str,
        date_patterns: list[str] | None = None,
    ) -> ChangeDetectionResult:
        """Check for changes by parsing publication date from a landing page.

        Args:
            source_id: Identifier for the source
            url: URL of the landing page to check
            date_patterns: Regex patterns to extract dates (uses defaults if None)

        Returns:
            ChangeDetectionResult with has_changed status
        """
        client = await self._get_client()
        state = self._load_state(source_id)

        if date_patterns is None:
            date_patterns = [
                r"[Ll]ast [Uu]pdated[:\s]+(\w+ \d{1,2},? \d{4})",
                r"[Uu]pdated[:\s]+(\d{1,2}/\d{1,2}/\d{4})",
                r"[Rr]eleased[:\s]+(\w+ \d{1,2},? \d{4})",
                r"[Dd]ata as of[:\s]+(\w+ \d{1,2},? \d{4})",
                r'"dateModified"[:\s]+"([^"]+)"',  # JSON-LD
            ]

        try:
            response = await client.get(url)
            response.raise_for_status()

            content = response.text
            publication_date: str | None = None

            for pattern in date_patterns:
                match = re.search(pattern, content)
                if match:
                    publication_date = match.group(1)
                    break

            if not publication_date:
                return ChangeDetectionResult(
                    url=url,
                    has_changed=True,
                    detection_method=ChangeDetectionMethod.PUBLICATION_DATE,
                    error_message="Could not find publication date on page",
                )

            previous_date = state.publication_date if state else None
            has_changed = previous_date is None or publication_date != previous_date

            if state is None:
                state = DataSourceState(source_id=source_id, url=url)
            state.publication_date = publication_date
            state.last_checked = datetime.now(UTC)
            if has_changed:
                state.last_changed = datetime.now(UTC)
            self._save_state(state)

            return ChangeDetectionResult(
                url=url,
                has_changed=has_changed,
                detection_method=ChangeDetectionMethod.PUBLICATION_DATE,
                current_value=publication_date,
                previous_value=previous_date,
            )

        except httpx.RequestError as e:
            self.logger.error(
                "Publication date check failed",
                url=url,
                error=str(e),
            )
            return ChangeDetectionResult(
                url=url,
                has_changed=True,
                detection_method=ChangeDetectionMethod.PUBLICATION_DATE,
                error_message=str(e),
            )

    async def check_rss_feed(
        self,
        source_id: str,
        feed_url: str,
    ) -> ChangeDetectionResult:
        """Check for changes using RSS/Atom feed.

        Args:
            source_id: Identifier for the source
            feed_url: URL of the RSS/Atom feed

        Returns:
            ChangeDetectionResult with has_changed status
        """
        try:
            import feedparser
        except ImportError:
            return ChangeDetectionResult(
                url=feed_url,
                has_changed=True,
                detection_method=ChangeDetectionMethod.RSS_FEED,
                error_message="feedparser not installed",
            )

        client = await self._get_client()
        state = self._load_state(source_id)

        try:
            response = await client.get(feed_url)
            response.raise_for_status()

            feed = feedparser.parse(response.text)

            if not feed.entries:
                return ChangeDetectionResult(
                    url=feed_url,
                    has_changed=True,
                    detection_method=ChangeDetectionMethod.RSS_FEED,
                    error_message="No entries in feed",
                )

            latest_entry = feed.entries[0]
            current_value = latest_entry.get("id") or latest_entry.get("link") or ""

            previous_value = state.metadata.get("latest_entry_id") if state else None
            has_changed = previous_value is None or current_value != previous_value

            if state is None:
                state = DataSourceState(source_id=source_id, url=feed_url)
            state.metadata["latest_entry_id"] = current_value
            state.metadata["latest_entry_title"] = latest_entry.get("title", "")
            state.last_checked = datetime.now(UTC)
            if has_changed:
                state.last_changed = datetime.now(UTC)
            self._save_state(state)

            return ChangeDetectionResult(
                url=feed_url,
                has_changed=has_changed,
                detection_method=ChangeDetectionMethod.RSS_FEED,
                current_value=latest_entry.get("title", current_value)[:50],
                previous_value=state.metadata.get("latest_entry_title", previous_value)[:50]
                if previous_value
                else None,
            )

        except httpx.RequestError as e:
            self.logger.error(
                "RSS feed check failed",
                url=feed_url,
                error=str(e),
            )
            return ChangeDetectionResult(
                url=feed_url,
                has_changed=True,
                detection_method=ChangeDetectionMethod.RSS_FEED,
                error_message=str(e),
            )

    async def check_for_changes(
        self,
        source_id: str,
        url: str,
        methods: list[ChangeDetectionMethod] | None = None,
    ) -> ChangeDetectionResult:
        """Check for changes using multiple methods, returning on first success.

        Args:
            source_id: Identifier for the source
            url: URL to check
            methods: List of detection methods to try (uses defaults if None)

        Returns:
            ChangeDetectionResult from the first successful method
        """
        if methods is None:
            methods = [
                ChangeDetectionMethod.ETAG,
                ChangeDetectionMethod.LAST_MODIFIED,
                ChangeDetectionMethod.CONTENT_HASH,
            ]

        for method in methods:
            if method == ChangeDetectionMethod.ETAG:
                result = await self.check_etag(source_id, url)
            elif method == ChangeDetectionMethod.LAST_MODIFIED:
                result = await self.check_last_modified(source_id, url)
            elif method == ChangeDetectionMethod.CONTENT_HASH:
                result = await self.check_content_hash(source_id, url)
            elif method == ChangeDetectionMethod.PUBLICATION_DATE:
                result = await self.check_publication_date(source_id, url)
            elif method == ChangeDetectionMethod.RSS_FEED:
                result = await self.check_rss_feed(source_id, url)
            else:
                continue

            if result.error_message is None:
                self.logger.info(
                    "Change detection result",
                    source_id=source_id,
                    method=method.value,
                    has_changed=result.has_changed,
                )
                return result

        # All methods failed or errored, assume changed
        self.logger.warning(
            "All change detection methods failed",
            source_id=source_id,
            url=url,
        )
        return ChangeDetectionResult(
            url=url,
            has_changed=True,
            detection_method=methods[0] if methods else ChangeDetectionMethod.CONTENT_HASH,
            error_message="All detection methods failed",
        )
