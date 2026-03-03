"""Archive manager for raw file storage with compression and retention.

Implements frequency-based retention policies:
- Daily data: 1 year raw retention, OR 250 GB size threshold
- Weekly data: 2 years raw retention
- Monthly data: 5 years raw retention
- Quarterly/Annual data: 8 years raw retention
- All archives: 8 years before permanent deletion
"""

import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()

# Try to import zstandard, fall back to gzip if not available
try:
    import zstandard as zstd

    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False
    import gzip


# Size threshold for daily data archival (in bytes)
DAILY_SIZE_THRESHOLD_BYTES = 250 * 1024 * 1024 * 1024  # 250 GB


@dataclass
class ArchivedFile:
    """Metadata about an archived file."""

    original_path: str
    archive_path: str
    source_id: str
    original_size: int
    compressed_size: int
    compression_ratio: float
    archived_at: datetime
    content_hash: str
    frequency: str = "unknown"  # daily, weekly, monthly, quarterly, annual, irregular
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchiveManifest:
    """Manifest of all archived files for a source."""

    source_id: str
    frequency: str = "unknown"
    files: dict[str, ArchivedFile] = field(default_factory=dict)
    total_original_size: int = 0
    total_compressed_size: int = 0
    total_raw_size: int = 0  # Current unarchived raw data size
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_new_data: datetime | None = None  # For irregular sources


class ArchiveManager:
    """Manages archival of raw data files with compression and retention.

    Features:
    - Compresses files using zstd (or gzip fallback)
    - Organizes archives by source and year
    - Frequency-based retention policies
    - Size-based archival triggers for daily data (250 GB threshold)
    - Special handling for irregular sources (archive old when new arrives)
    - Provides restoration capability
    - Maintains archive manifest for tracking
    """

    ARCHIVE_RETENTION_YEARS = 8  # How long to keep compressed archives
    
    # Raw data retention by frequency (in days)
    RAW_RETENTION_DAYS = {
        "real_time": 365,   # 1 year
        "daily": 365,       # 1 year
        "weekly": 730,      # 2 years
        "monthly": 1825,    # 5 years
        "quarterly": 2920,  # 8 years
        "annual": 2920,     # 8 years
        "irregular": 365,   # 1 year of prior versions
    }
    
    # Size thresholds for archival triggers (in bytes)
    SIZE_THRESHOLDS = {
        "real_time": DAILY_SIZE_THRESHOLD_BYTES,  # 250 GB
        "daily": DAILY_SIZE_THRESHOLD_BYTES,       # 250 GB
        "weekly": None,
        "monthly": None,
        "quarterly": None,
        "annual": None,
        "irregular": None,
    }

    def __init__(
        self,
        data_dir: Path | None = None,
        compression_level: int = 3,
        retention_years: int | None = None,
    ):
        """Initialize the archive manager.

        Args:
            data_dir: Base data directory (contains raw/, archive/, manifests/)
            compression_level: zstd compression level (1-22, default 3)
            retention_years: Years to retain archives (default 8)
        """
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.archive_dir = self.data_dir / "archive"
        self.manifest_dir = self.data_dir / "manifests"
        self.compression_level = compression_level
        self.archive_retention_years = retention_years or self.ARCHIVE_RETENTION_YEARS
        self.logger = logger.bind(component="ArchiveManager")

        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

    def get_raw_retention_days(self, frequency: str) -> int:
        """Get raw data retention period for a frequency tier."""
        return self.RAW_RETENTION_DAYS.get(frequency.lower(), 365)

    def get_size_threshold(self, frequency: str) -> int | None:
        """Get size threshold for a frequency tier (in bytes)."""
        return self.SIZE_THRESHOLDS.get(frequency.lower())

    @staticmethod
    def _sanitize_source_id(source_id: str) -> str:
        """Sanitize source_id to prevent path traversal."""
        return source_id.replace("/", "").replace("\\", "").replace("..", "")

    def get_source_raw_size(self, source_id: str) -> int:
        """Calculate total size of raw (unarchived) data for a source.

        Args:
            source_id: The data source identifier

        Returns:
            Total size in bytes
        """
        source_id = self._sanitize_source_id(source_id)
        source_raw_dir = self.raw_dir / source_id
        if not source_raw_dir.exists():
            return 0

        total_size = 0
        for file_path in source_raw_dir.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size

        return total_size

    def check_size_threshold(self, source_id: str, frequency: str) -> bool:
        """Check if a source has exceeded its size threshold.

        Args:
            source_id: The data source identifier
            frequency: The data frequency tier

        Returns:
            True if threshold exceeded, False otherwise
        """
        threshold = self.get_size_threshold(frequency)
        if threshold is None:
            return False

        current_size = self.get_source_raw_size(source_id)
        exceeded = current_size >= threshold

        if exceeded:
            self.logger.warning(
                "Size threshold exceeded",
                source_id=source_id,
                current_size_gb=round(current_size / (1024**3), 2),
                threshold_gb=round(threshold / (1024**3), 2),
            )

        return exceeded

    def archive_old_for_irregular_source(
        self,
        source_id: str,
        new_data_path: Path,
    ) -> list[ArchivedFile]:
        """Archive old data when new data arrives for irregular sources.

        This is for sources like MARAD that don't have fixed update schedules.
        When new data is detected, archive all existing data.

        Args:
            source_id: The data source identifier
            new_data_path: Path to the new data file

        Returns:
            List of archived files
        """
        source_id = self._sanitize_source_id(source_id)
        archived_files: list[ArchivedFile] = []
        source_raw_dir = self.raw_dir / source_id

        if not source_raw_dir.exists():
            return archived_files

        for file_path in source_raw_dir.rglob("*"):
            if file_path.is_file() and file_path != new_data_path:
                try:
                    archived = self.archive_raw_file(
                        file_path=file_path,
                        source_id=source_id,
                        frequency="irregular",
                        delete_original=True,
                    )
                    archived_files.append(archived)
                except Exception as e:
                    self.logger.error(
                        "Failed to archive file for irregular source",
                        source_id=source_id,
                        file=str(file_path),
                        error=str(e),
                    )

        manifest = self._load_manifest(source_id)
        manifest.last_new_data = datetime.now(UTC)
        self._save_manifest(manifest)

        self.logger.info(
            "Archived old data for irregular source",
            source_id=source_id,
            archived_count=len(archived_files),
            new_data=str(new_data_path),
        )

        return archived_files

    def _get_archive_path(
        self,
        source_id: str,
        original_filename: str,
        date: datetime | None = None,
    ) -> Path:
        """Generate archive path with year-based organization.

        Args:
            source_id: The data source identifier
            original_filename: Original file name
            date: Date for organizing (defaults to now)

        Returns:
            Path to the archive file
        """
        date = date or datetime.now(UTC)
        year = str(date.year)

        archive_subdir = self.archive_dir / source_id / year
        archive_subdir.mkdir(parents=True, exist_ok=True)

        date_prefix = date.strftime("%Y-%m-%d")
        extension = ".zst" if ZSTD_AVAILABLE else ".gz"

        return archive_subdir / f"{date_prefix}_{original_filename}{extension}"

    def _compress_file(self, content: bytes) -> bytes:
        """Compress content using zstd or gzip.

        Args:
            content: Raw bytes to compress

        Returns:
            Compressed bytes
        """
        if ZSTD_AVAILABLE:
            cctx = zstd.ZstdCompressor(level=self.compression_level)
            return cctx.compress(content)
        else:
            return gzip.compress(content, compresslevel=self.compression_level)

    def _decompress_file(self, compressed: bytes) -> bytes:
        """Decompress content.

        Args:
            compressed: Compressed bytes

        Returns:
            Decompressed bytes
        """
        if ZSTD_AVAILABLE:
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(compressed)
        else:
            return gzip.decompress(compressed)

    def _load_manifest(self, source_id: str) -> ArchiveManifest:
        """Load archive manifest for a source."""
        source_id = self._sanitize_source_id(source_id)
        manifest_path = self.manifest_dir / f"{source_id}_archive.json"

        if not manifest_path.exists():
            return ArchiveManifest(source_id=source_id)

        try:
            data = json.loads(manifest_path.read_text())
            manifest = ArchiveManifest(
                source_id=data["source_id"],
                frequency=data.get("frequency", "unknown"),
                total_original_size=data.get("total_original_size", 0),
                total_compressed_size=data.get("total_compressed_size", 0),
                total_raw_size=data.get("total_raw_size", 0),
                last_updated=datetime.fromisoformat(data["last_updated"]),
                last_new_data=(
                    datetime.fromisoformat(data["last_new_data"])
                    if data.get("last_new_data")
                    else None
                ),
            )

            for key, file_data in data.get("files", {}).items():
                manifest.files[key] = ArchivedFile(
                    original_path=file_data["original_path"],
                    archive_path=file_data["archive_path"],
                    source_id=file_data["source_id"],
                    original_size=file_data["original_size"],
                    compressed_size=file_data["compressed_size"],
                    compression_ratio=file_data["compression_ratio"],
                    archived_at=datetime.fromisoformat(file_data["archived_at"]),
                    content_hash=file_data["content_hash"],
                    frequency=file_data.get("frequency", "unknown"),
                    metadata=file_data.get("metadata", {}),
                )

            return manifest
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.warning(
                "Failed to load archive manifest",
                source_id=source_id,
                error=str(e),
            )
            return ArchiveManifest(source_id=source_id)

    def _save_manifest(self, manifest: ArchiveManifest) -> None:
        """Save archive manifest."""
        manifest_path = self.manifest_dir / f"{manifest.source_id}_archive.json"

        data = {
            "source_id": manifest.source_id,
            "frequency": manifest.frequency,
            "total_original_size": manifest.total_original_size,
            "total_compressed_size": manifest.total_compressed_size,
            "total_raw_size": manifest.total_raw_size,
            "last_updated": manifest.last_updated.isoformat(),
            "last_new_data": manifest.last_new_data.isoformat() if manifest.last_new_data else None,
            "files": {},
        }

        for key, archived in manifest.files.items():
            data["files"][key] = {
                "original_path": archived.original_path,
                "archive_path": archived.archive_path,
                "source_id": archived.source_id,
                "original_size": archived.original_size,
                "compressed_size": archived.compressed_size,
                "compression_ratio": archived.compression_ratio,
                "archived_at": archived.archived_at.isoformat(),
                "content_hash": archived.content_hash,
                "frequency": archived.frequency,
                "metadata": archived.metadata,
            }

        temp_path = manifest_path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(data, indent=2))
        temp_path.rename(manifest_path)

    def archive_raw_file(
        self,
        file_path: Path,
        source_id: str,
        content_hash: str | None = None,
        metadata: dict[str, Any] | None = None,
        frequency: str = "unknown",
        delete_original: bool = True,
    ) -> ArchivedFile:
        """Compress and archive a raw file.

        Args:
            file_path: Path to the raw file
            source_id: The data source identifier
            content_hash: Optional pre-computed content hash
            metadata: Optional metadata to store with archive
            frequency: Data frequency tier (daily, weekly, etc.)
            delete_original: Whether to delete the original file after archiving

        Returns:
            ArchivedFile with archive details
        """
        source_id = self._sanitize_source_id(source_id)
        import hashlib

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        content = file_path.read_bytes()
        original_size = len(content)

        if not content_hash:
            content_hash = hashlib.sha256(content).hexdigest()

        compressed = self._compress_file(content)
        compressed_size = len(compressed)
        compression_ratio = (
            (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        )

        archive_path = self._get_archive_path(
            source_id=source_id,
            original_filename=file_path.name,
        )

        archive_path.write_bytes(compressed)

        archived = ArchivedFile(
            original_path=str(file_path),
            archive_path=str(archive_path),
            source_id=source_id,
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=round(compression_ratio, 2),
            archived_at=datetime.now(UTC),
            content_hash=content_hash,
            frequency=frequency,
            metadata=metadata or {},
        )

        manifest = self._load_manifest(source_id)
        manifest.frequency = frequency
        manifest.files[content_hash] = archived
        manifest.total_original_size += original_size
        manifest.total_compressed_size += compressed_size
        manifest.last_updated = datetime.now(UTC)
        self._save_manifest(manifest)

        self.logger.info(
            "File archived",
            original=str(file_path),
            archive=str(archive_path),
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=f"{compression_ratio:.1f}%",
            frequency=frequency,
        )

        if delete_original:
            file_path.unlink()
            self.logger.debug("Original file deleted", path=str(file_path))

        return archived

    def restore_from_archive(
        self,
        archive_path: Path | str,
        restore_path: Path | str | None = None,
    ) -> Path:
        """Restore a file from archive.

        Args:
            archive_path: Path to the archived file
            restore_path: Where to restore (defaults to original location)

        Returns:
            Path to the restored file
        """
        archive_path = Path(archive_path)

        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")

        compressed = archive_path.read_bytes()
        content = self._decompress_file(compressed)

        if restore_path is None:
            filename = archive_path.name
            if filename.endswith(".zst"):
                filename = filename[:-4]
            elif filename.endswith(".gz"):
                filename = filename[:-3]
            if len(filename) > 11 and filename[10] == "_":
                filename = filename[11:]

            restore_path = self.raw_dir / "restored" / filename
        else:
            restore_path = Path(restore_path)

        restore_path.parent.mkdir(parents=True, exist_ok=True)
        restore_path.write_bytes(content)

        self.logger.info(
            "File restored from archive",
            archive=str(archive_path),
            restored=str(restore_path),
            size=len(content),
        )

        return restore_path

    def enforce_raw_retention(
        self,
        source_id: str,
        frequency: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Archive raw files that exceed the retention period for their frequency.

        Args:
            source_id: The data source identifier
            frequency: Data frequency tier (daily, weekly, etc.)
            dry_run: If True, only report what would be archived

        Returns:
            Summary of archival operation
        """
        retention_days = self.get_raw_retention_days(frequency)
        cutoff_date = datetime.now(UTC) - timedelta(days=retention_days)
        
        source_raw_dir = self.raw_dir / source_id
        if not source_raw_dir.exists():
            return {"source_id": source_id, "archived_count": 0, "archived_bytes": 0}

        archived_count = 0
        archived_bytes = 0
        errors: list[str] = []

        for file_path in source_raw_dir.rglob("*"):
            if not file_path.is_file():
                continue

            try:
                file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                if file_mtime < cutoff_date:
                    file_size = file_path.stat().st_size
                    
                    if dry_run:
                        self.logger.info(
                            "Would archive (dry run)",
                            path=str(file_path),
                            size=file_size,
                            age_days=(datetime.now(UTC) - file_mtime).days,
                        )
                    else:
                        self.archive_raw_file(
                            file_path=file_path,
                            source_id=source_id,
                            frequency=frequency,
                            delete_original=True,
                        )
                    
                    archived_count += 1
                    archived_bytes += file_size

            except Exception as e:
                errors.append(f"{file_path}: {e}")
                self.logger.error(
                    "Error archiving file",
                    path=str(file_path),
                    error=str(e),
                )

        summary = {
            "source_id": source_id,
            "frequency": frequency,
            "retention_days": retention_days,
            "cutoff_date": cutoff_date.isoformat(),
            "archived_count": archived_count,
            "archived_bytes": archived_bytes,
            "archived_mb": round(archived_bytes / (1024 * 1024), 2),
            "dry_run": dry_run,
            "errors": errors,
        }

        self.logger.info("Raw retention enforced", **summary)
        return summary

    def enforce_size_threshold(
        self,
        source_id: str,
        frequency: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Delete oldest raw files if size threshold is exceeded.

        When daily/real-time data exceeds 250 GB, oldest files are permanently
        deleted (not archived) to maintain storage limits.

        Args:
            source_id: The data source identifier
            frequency: Data frequency tier (daily, weekly, etc.)
            dry_run: If True, only report what would be deleted

        Returns:
            Summary of deletion operation
        """
        threshold = self.get_size_threshold(frequency)
        if threshold is None:
            return {"source_id": source_id, "threshold": None, "deleted_count": 0}

        current_size = self.get_source_raw_size(source_id)
        if current_size < threshold:
            return {
                "source_id": source_id,
                "current_size_gb": round(current_size / (1024**3), 2),
                "threshold_gb": round(threshold / (1024**3), 2),
                "deleted_count": 0,
                "message": "Under threshold",
            }

        source_raw_dir = self.raw_dir / source_id
        if not source_raw_dir.exists():
            return {"source_id": source_id, "deleted_count": 0}

        files_by_age: list[tuple[Path, datetime, int]] = []
        for file_path in source_raw_dir.rglob("*"):
            if file_path.is_file():
                stat = file_path.stat()
                files_by_age.append((
                    file_path,
                    datetime.fromtimestamp(stat.st_mtime),
                    stat.st_size,
                ))

        files_by_age.sort(key=lambda x: x[1])

        deleted_count = 0
        deleted_bytes = 0
        remaining_size = current_size

        # Delete oldest files until under threshold
        target_size = int(threshold * 0.8)  # Target 80% of threshold
        
        for file_path, mtime, file_size in files_by_age:
            if remaining_size <= target_size:
                break

            try:
                if dry_run:
                    self.logger.info(
                        "Would delete (dry run - size threshold)",
                        path=str(file_path),
                        size=file_size,
                        age_days=(datetime.now(UTC) - mtime).days,
                    )
                else:
                    # Permanently delete - no archiving for size threshold
                    file_path.unlink()
                    self.logger.warning(
                        "Deleted file due to size threshold",
                        path=str(file_path),
                        size=file_size,
                        age_days=(datetime.now(UTC) - mtime).days,
                    )

                deleted_count += 1
                deleted_bytes += file_size
                remaining_size -= file_size

            except Exception as e:
                self.logger.error(
                    "Error deleting file for size threshold",
                    path=str(file_path),
                    error=str(e),
                )

        summary = {
            "source_id": source_id,
            "frequency": frequency,
            "initial_size_gb": round(current_size / (1024**3), 2),
            "threshold_gb": round(threshold / (1024**3), 2),
            "final_size_gb": round(remaining_size / (1024**3), 2),
            "deleted_count": deleted_count,
            "deleted_bytes": deleted_bytes,
            "deleted_gb": round(deleted_bytes / (1024**3), 2),
            "dry_run": dry_run,
        }

        self.logger.warning("Size threshold enforced - files deleted", **summary)
        return summary

    def cleanup_old_archives(
        self,
        cutoff_date: datetime | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Delete archives older than the archive retention period (8 years).

        Args:
            cutoff_date: Delete archives before this date (defaults to 8 years ago)
            dry_run: If True, only report what would be deleted

        Returns:
            Summary of cleanup operation
        """
        if cutoff_date is None:
            cutoff_date = datetime.now(UTC) - timedelta(days=self.archive_retention_years * 365)

        deleted_count = 0
        deleted_bytes = 0
        errors: list[str] = []

        for source_dir in self.archive_dir.iterdir():
            if not source_dir.is_dir():
                continue

            source_id = self._sanitize_source_id(source_dir.name)
            manifest = self._load_manifest(source_id)

            for year_dir in source_dir.iterdir():
                if not year_dir.is_dir():
                    continue

                try:
                    year = int(year_dir.name)
                    if year < cutoff_date.year:
                        for file_path in year_dir.iterdir():
                            file_size = file_path.stat().st_size

                            if dry_run:
                                self.logger.info(
                                    "Would delete (dry run)",
                                    path=str(file_path),
                                    size=file_size,
                                )
                            else:
                                file_path.unlink()
                                self.logger.debug("Deleted archive", path=str(file_path))

                            deleted_count += 1
                            deleted_bytes += file_size

                        if not dry_run:
                            year_dir.rmdir()

                except ValueError:
                    # Not a year directory, skip
                    continue
                except Exception as e:
                    errors.append(f"{year_dir}: {e}")
                    self.logger.error(
                        "Error cleaning up archives",
                        path=str(year_dir),
                        error=str(e),
                    )

            if not dry_run:
                try:
                    if not any(source_dir.iterdir()):
                        source_dir.rmdir()
                except OSError:
                    pass  # Directory not empty, that's fine

        summary = {
            "cutoff_date": cutoff_date.isoformat(),
            "deleted_count": deleted_count,
            "deleted_bytes": deleted_bytes,
            "deleted_mb": round(deleted_bytes / (1024 * 1024), 2),
            "dry_run": dry_run,
            "errors": errors,
        }

        self.logger.info(
            "Archive cleanup completed",
            **summary,
        )

        return summary

    def get_archive_stats(self, source_id: str | None = None) -> dict[str, Any]:
        """Get statistics about archived files.

        Args:
            source_id: Optional source to filter by

        Returns:
            Dictionary with archive statistics
        """
        if source_id:
            source_id = self._sanitize_source_id(source_id)
        stats: dict[str, Any] = {
            "sources": {},
            "total_original_size": 0,
            "total_compressed_size": 0,
            "total_files": 0,
            "overall_compression_ratio": 0,
        }

        sources = [source_id] if source_id else [
            d.name for d in self.archive_dir.iterdir() if d.is_dir()
        ]

        for src_id in sources:
            manifest = self._load_manifest(src_id)
            source_stats = {
                "file_count": len(manifest.files),
                "original_size": manifest.total_original_size,
                "compressed_size": manifest.total_compressed_size,
                "compression_ratio": (
                    round(
                        (1 - manifest.total_compressed_size / manifest.total_original_size) * 100,
                        2,
                    )
                    if manifest.total_original_size > 0
                    else 0
                ),
                "last_updated": manifest.last_updated.isoformat(),
            }

            stats["sources"][src_id] = source_stats
            stats["total_original_size"] += manifest.total_original_size
            stats["total_compressed_size"] += manifest.total_compressed_size
            stats["total_files"] += len(manifest.files)

        if stats["total_original_size"] > 0:
            stats["overall_compression_ratio"] = round(
                (1 - stats["total_compressed_size"] / stats["total_original_size"]) * 100,
                2,
            )

        return stats

    def get_archive_manifest(
        self,
        source_id: str,
    ) -> list[dict[str, Any]]:
        """List all archived files for a source.

        Args:
            source_id: The data source identifier

        Returns:
            List of archived file metadata
        """
        manifest = self._load_manifest(source_id)
        return [
            {
                "archive_path": f.archive_path,
                "original_path": f.original_path,
                "original_size": f.original_size,
                "compressed_size": f.compressed_size,
                "compression_ratio": f.compression_ratio,
                "archived_at": f.archived_at.isoformat(),
                "content_hash": f.content_hash,
                "metadata": f.metadata,
            }
            for f in manifest.files.values()
        ]
