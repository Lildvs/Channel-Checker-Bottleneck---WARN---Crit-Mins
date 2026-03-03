"""File-based collector for download-only data sources."""

import hashlib
import io
import json
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import structlog

from src.data_ingestion.base_collector import (
    BaseCollector,
    CollectionResult,
    DataFrequency,
    DataPoint,
)

logger = structlog.get_logger()


class FileFormat(str, Enum):
    """Supported file formats for download."""

    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    XML = "xml"


@dataclass
class DownloadedFile:
    """Represents a downloaded file with metadata."""

    url: str
    content: bytes
    filename: str
    format: FileFormat
    content_hash: str
    etag: str | None = None
    last_modified: str | None = None
    downloaded_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    size_bytes: int = 0

    @classmethod
    def from_response(
        cls,
        url: str,
        response: httpx.Response,
        filename: str,
        file_format: FileFormat,
    ) -> "DownloadedFile":
        """Create a DownloadedFile from an HTTP response."""
        content = response.content
        content_hash = hashlib.sha256(content).hexdigest()

        return cls(
            url=url,
            content=content,
            filename=filename,
            format=file_format,
            content_hash=content_hash,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            size_bytes=len(content),
        )


@dataclass
class DatasetConfig:
    """Configuration for a dataset to download."""

    dataset_id: str
    url: str
    format: FileFormat
    filename: str | None = None
    description: str | None = None
    expected_frequency: DataFrequency = DataFrequency.MONTHLY
    parser_options: dict[str, Any] = field(default_factory=dict)


class FileBasedCollector(BaseCollector):
    """Base class for file-based data collection.

    This extends BaseCollector with capabilities for:
    - Downloading files via HTTP/HTTPS with retry logic
    - Parsing multiple file formats (CSV, Excel, JSON)
    - Change detection via ETag/Last-Modified headers and content hashing
    - Authentication handling
    """

    def __init__(
        self,
        name: str,
        source_id: str,
        data_dir: Path | None = None,
        timeout: float = 60.0,
        max_retries: int = 3,
    ):
        """Initialize the file-based collector.

        Args:
            name: Human-readable name for the collector
            source_id: Unique identifier for the data source
            data_dir: Base directory for storing downloaded files
            timeout: HTTP request timeout in seconds
            max_retries: Maximum number of retry attempts for failed downloads
        """
        super().__init__(name, source_id)
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw" / source_id
        self.manifest_path = self.data_dir / "manifests" / f"{source_id}.json"
        self.timeout = timeout
        self.max_retries = max_retries

        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
                headers=self._get_default_headers(),
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _get_default_headers(self) -> dict[str, str]:
        """Get default headers for HTTP requests.

        Override in subclasses to add authentication or custom headers.
        """
        return {
            "User-Agent": "ChannelCheckResearcher/1.0 (Economic Research Tool)",
            "Accept": "*/*",
        }

    @abstractmethod
    def get_datasets(self) -> list[DatasetConfig]:
        """Get the list of datasets to collect.

        Returns:
            List of dataset configurations
        """
        ...

    @abstractmethod
    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        """Convert a parsed DataFrame to DataPoints.

        Args:
            df: Parsed pandas DataFrame
            dataset_id: The dataset identifier

        Returns:
            List of DataPoint objects
        """
        ...

    async def download_file(
        self,
        url: str,
        filename: str,
        file_format: FileFormat,
        check_modified: bool = True,
    ) -> DownloadedFile | None:
        """Download a file from a URL.

        Args:
            url: URL to download from
            filename: Name to save the file as
            file_format: Expected file format
            check_modified: If True, check if file has changed before downloading

        Returns:
            DownloadedFile if successful or new content, None if unchanged
        """
        client = await self._get_client()

        # Check if we should skip based on ETag/Last-Modified
        if check_modified:
            manifest = self._load_manifest()
            cached_info = manifest.get("datasets", {}).get(filename, {})

            # Try HEAD request first to check if modified
            try:
                head_response = await client.head(url)
                if head_response.status_code == 200:
                    current_etag = head_response.headers.get("etag")
                    current_modified = head_response.headers.get("last-modified")

                    if (
                        cached_info.get("etag")
                        and cached_info.get("etag") == current_etag
                    ):
                        self.logger.info(
                            "File unchanged (ETag match)",
                            url=url,
                            etag=current_etag,
                        )
                        return None
            except httpx.RequestError:
                # HEAD not supported or failed, proceed with GET
                pass

        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                self.logger.info(
                    "Downloading file",
                    url=url,
                    attempt=attempt + 1,
                    max_retries=self.max_retries,
                )
                response = await client.get(url)
                response.raise_for_status()

                downloaded = DownloadedFile.from_response(
                    url=url,
                    response=response,
                    filename=filename,
                    file_format=file_format,
                )

                if check_modified:
                    manifest = self._load_manifest()
                    cached_hash = manifest.get("datasets", {}).get(filename, {}).get(
                        "content_hash"
                    )
                    if cached_hash == downloaded.content_hash:
                        self.logger.info(
                            "File unchanged (hash match)",
                            url=url,
                            hash=downloaded.content_hash[:16],
                        )
                        return None

                self.logger.info(
                    "File downloaded successfully",
                    url=url,
                    size_bytes=downloaded.size_bytes,
                    hash=downloaded.content_hash[:16],
                )
                return downloaded

            except httpx.HTTPStatusError as e:
                last_error = e
                self.logger.warning(
                    "HTTP error downloading file",
                    url=url,
                    status_code=e.response.status_code,
                    attempt=attempt + 1,
                )
            except httpx.RequestError as e:
                last_error = e
                self.logger.warning(
                    "Request error downloading file",
                    url=url,
                    error=str(e),
                    attempt=attempt + 1,
                )

        self.logger.error(
            "Failed to download file after retries",
            url=url,
            max_retries=self.max_retries,
            error=str(last_error),
        )
        raise last_error or Exception(f"Failed to download {url}")

    def parse_file(
        self,
        downloaded: DownloadedFile,
        **parser_options: Any,
    ) -> pd.DataFrame:
        """Parse a downloaded file into a DataFrame.

        Args:
            downloaded: The downloaded file
            **parser_options: Additional options passed to the parser

        Returns:
            Parsed pandas DataFrame
        """
        content_io = io.BytesIO(downloaded.content)

        if downloaded.format == FileFormat.CSV:
            return pd.read_csv(content_io, **parser_options)
        elif downloaded.format == FileFormat.EXCEL:
            return pd.read_excel(content_io, engine="openpyxl", **parser_options)
        elif downloaded.format == FileFormat.JSON:
            return pd.read_json(content_io, **parser_options)
        elif downloaded.format == FileFormat.XML:
            return pd.read_xml(content_io, **parser_options)
        else:
            raise ValueError(f"Unsupported file format: {downloaded.format}")

    def save_raw_file(self, downloaded: DownloadedFile) -> Path:
        """Save a downloaded file to the raw directory.

        Args:
            downloaded: The downloaded file

        Returns:
            Path to the saved file
        """
        date_str = downloaded.downloaded_at.strftime("%Y-%m-%d")
        save_dir = self.raw_dir / date_str
        save_dir.mkdir(parents=True, exist_ok=True)

        file_path = save_dir / downloaded.filename
        file_path.write_bytes(downloaded.content)

        self.logger.info(
            "Saved raw file",
            path=str(file_path),
            size_bytes=downloaded.size_bytes,
        )
        return file_path

    def _load_manifest(self) -> dict[str, Any]:
        """Load the manifest file."""
        if self.manifest_path.exists():
            return json.loads(self.manifest_path.read_text())
        return {"source": self.source_id, "datasets": {}}

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        """Save the manifest file."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(json.dumps(manifest, indent=2, default=str))

    def update_manifest(
        self,
        downloaded: DownloadedFile,
        file_path: Path,
    ) -> None:
        """Update the manifest with new download information.

        Args:
            downloaded: The downloaded file
            file_path: Path where the file was saved
        """
        manifest = self._load_manifest()

        manifest["datasets"][downloaded.filename] = {
            "url": downloaded.url,
            "last_checked": datetime.now(UTC).isoformat(),
            "last_modified": downloaded.last_modified,
            "etag": downloaded.etag,
            "content_hash": downloaded.content_hash,
            "last_downloaded": downloaded.downloaded_at.isoformat(),
            "file_path": str(file_path),
            "size_bytes": downloaded.size_bytes,
        }

        self._save_manifest(manifest)
        self.logger.info("Manifest updated", filename=downloaded.filename)

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data by downloading and parsing files.

        Args:
            series_ids: Optional list of dataset IDs to collect (defaults to all)
            start_date: Not used for file-based collection
            end_date: Not used for file-based collection

        Returns:
            List of collected data points
        """
        all_data_points: list[DataPoint] = []
        datasets = self.get_datasets()

        if series_ids:
            datasets = [d for d in datasets if d.dataset_id in series_ids]

        for dataset in datasets:
            try:
                filename = dataset.filename or f"{dataset.dataset_id}.{dataset.format.value}"

                downloaded = await self.download_file(
                    url=dataset.url,
                    filename=filename,
                    file_format=dataset.format,
                    check_modified=True,
                )

                if downloaded is None:
                    self.logger.info(
                        "Dataset unchanged, skipping",
                        dataset_id=dataset.dataset_id,
                    )
                    continue

                file_path = self.save_raw_file(downloaded)
                df = self.parse_file(downloaded, **dataset.parser_options)
                data_points = self.parse_dataframe_to_datapoints(df, dataset.dataset_id)
                all_data_points.extend(data_points)
                self.update_manifest(downloaded, file_path)

                self.logger.info(
                    "Dataset processed",
                    dataset_id=dataset.dataset_id,
                    records=len(data_points),
                )

            except Exception as e:
                self.logger.error(
                    "Failed to process dataset",
                    dataset_id=dataset.dataset_id,
                    error=str(e),
                )
                continue

        return all_data_points

    async def run_collection(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> CollectionResult:
        """Run a full collection cycle with cleanup.

        Overrides base to ensure HTTP client is closed after collection.
        """
        try:
            return await super().run_collection(series_ids, start_date, end_date)
        finally:
            await self.close()

    def get_default_series(self) -> list[str]:
        """Get the default list of dataset IDs to collect."""
        return [d.dataset_id for d in self.get_datasets()]
