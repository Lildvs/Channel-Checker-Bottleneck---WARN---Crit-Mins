"""Shipping Data collector for port throughput and container metrics.

Collects from two sources:

1. **Port of Los Angeles** -- monthly TEU counts via LA Open Data CSV.
2. **BTS Port Performance** -- annual port rankings (tonnage, dry bulk,
   container TEU) extracted from the BTS PDF report published each
   January.

Data Sources:
    Port of LA: https://data.lacity.org/api/views/tsuv-4rgh/rows.csv
    BTS PDF:    https://www.bts.gov/.../BTS_Port-Performance-<YEAR>_Annual-Report_...pdf

The BTS site uses Akamai WAF that intermittently blocks automated
downloads.  The collector tries ``subprocess`` calls to ``wget`` then
``curl`` with browser-like headers, and caches the PDF locally to
avoid re-downloading on every run.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pdfplumber
import structlog

from src.data_ingestion.base_collector import DataFrequency, DataPoint
from src.data_ingestion.file_collector import (
    DatasetConfig,
    FileBasedCollector,
    FileFormat,
)

logger = structlog.get_logger()

POLA_COLUMN_MAP: dict[str, str] = {
    "Monthly Total TEUs": "MONTHLY_TOTAL",
    "CYTD Total TEUs": "CYTD_TOTAL",
    "% Change Total TEUs CYTD": "CYTD_PCT_CHANGE",
}

BTS_REPORT_URLS: dict[int, str] = {
    2026: (
        "https://www.bts.gov/sites/bts.dot.gov/files/2026-01/"
        "BTS_Port-Performance-2026_Annual-Report_Final%2011326.pdf"
    ),
    2025: (
        "https://www.bts.gov/sites/bts.dot.gov/files/2025-03/"
        "BTS_Port-Performance-2025_Annual-Report_for%20web_20250224-1232.pdf"
    ),
    2024: (
        "https://www.bts.gov/sites/bts.dot.gov/files/2024-01/"
        "2024%20Port%20Performance%20Annual%20Report.pdf"
    ),
}

_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_RANKING_TABLE_RE = re.compile(
    r"^\s*(\d{1,3})\s+"
    r"(.+?)\s+"
    r"([\d,]+(?:\.\d+)?)\s*$",
)


class ShippingDataCollector(FileBasedCollector):
    """Collector for Port of LA TEU + BTS port performance data."""

    POLA_API_URL = (
        "https://data.lacity.org/api/views/tsuv-4rgh/rows.csv"
        "?accessType=DOWNLOAD"
    )

    def __init__(self) -> None:
        super().__init__(
            name="Shipping Data",
            source_id="shipping",
            timeout=120.0,
            max_retries=3,
        )
        self._bts_cache_dir = self.data_dir / "raw" / "bts_port"

    @property
    def frequency(self) -> DataFrequency:
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        return "0 14 16 * *"

    def get_datasets(self) -> list[DatasetConfig]:
        return [
            DatasetConfig(
                dataset_id="POLA_TEU",
                url=self.POLA_API_URL,
                format=FileFormat.CSV,
                filename="pola_teu_monthly.csv",
                description="Port of Los Angeles Monthly TEU Statistics",
                expected_frequency=DataFrequency.MONTHLY,
                parser_options={},
            ),
        ]

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        pola_points = await super().collect(
            series_ids=series_ids,
            start_date=start_date,
            end_date=end_date,
        )

        bts_points = await self._collect_bts_reports()

        all_points = pola_points + bts_points
        self.logger.info(
            "Shipping collection complete",
            pola=len(pola_points),
            bts=len(bts_points),
            total=len(all_points),
        )
        return all_points

    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        if dataset_id == "POLA_TEU":
            return self._parse_pola_teu(df)

        self.logger.warning("Unknown dataset", dataset_id=dataset_id)
        return []

    def _parse_pola_teu(self, df: pd.DataFrame) -> list[DataPoint]:
        data_points: list[DataPoint] = []
        self.logger.debug("POLA columns received", columns=list(df.columns))

        date_col = self._find_date_column(df)
        if date_col is None:
            self.logger.warning(
                "Could not find date column in POLA data",
                columns=list(df.columns),
            )
            return data_points

        value_columns = self._map_value_columns(df)
        if not value_columns:
            self.logger.warning(
                "No numeric TEU columns found in POLA data",
                columns=list(df.columns),
            )
            return data_points

        for _, row in df.iterrows():
            try:
                ts = self._parse_row_timestamp(row, date_col)
                if ts is None:
                    continue

                for series_suffix, col_name in value_columns.items():
                    raw = row.get(col_name)
                    if pd.isna(raw):
                        continue
                    try:
                        value = float(raw)
                    except (ValueError, TypeError):
                        continue

                    unit = "percent" if "PCT" in series_suffix else "TEU"
                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=f"POLA_TEU_{series_suffix}",
                            timestamp=ts,
                            value=value,
                            unit=unit,
                            metadata={
                                "port": "Port of Los Angeles",
                                "port_code": "POLA",
                                "original_column": col_name,
                            },
                        )
                    )
            except Exception as e:
                self.logger.debug("Failed to parse POLA row", error=str(e))

        self.logger.info(
            "Parsed POLA TEU data",
            total_records=len(data_points),
            metrics=list(value_columns.keys()),
        )
        return data_points

    async def _collect_bts_reports(self) -> list[DataPoint]:
        """Download (if needed) and parse BTS port performance PDFs."""
        all_points: list[DataPoint] = []
        self._bts_cache_dir.mkdir(parents=True, exist_ok=True)

        for report_year, url in BTS_REPORT_URLS.items():
            cached = self._bts_cache_dir / f"bts_port_performance_{report_year}.pdf"

            if not cached.exists() or cached.stat().st_size < 100_000:
                pdf_bytes = await self._download_bts_pdf(url)
                if pdf_bytes is None:
                    self.logger.warning(
                        "BTS PDF download failed, skipping",
                        year=report_year,
                    )
                    continue
                cached.write_bytes(pdf_bytes)
                self.logger.info(
                    "BTS PDF cached",
                    year=report_year,
                    size=len(pdf_bytes),
                )

            try:
                points = self._extract_bts_data(cached, report_year)
                all_points.extend(points)
            except Exception as e:
                self.logger.error(
                    "BTS PDF extraction failed",
                    year=report_year,
                    error=str(e),
                )

        return all_points

    async def _download_bts_pdf(self, url: str) -> bytes | None:
        """Download a BTS PDF, trying multiple strategies to bypass WAF."""
        for strategy in [self._download_via_wget, self._download_via_curl, self._download_via_httpx]:
            try:
                data = await strategy(url)
                if data and len(data) > 100_000 and data[:5] == b"%PDF-":
                    return data
            except Exception as e:
                self.logger.debug(
                    "BTS download strategy failed",
                    strategy=strategy.__name__,
                    error=str(e),
                )
        return None

    async def _download_via_wget(self, url: str) -> bytes | None:
        try:
            result = subprocess.run(
                ["wget", "-q", "-O", "-", url],
                capture_output=True,
                timeout=120,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except FileNotFoundError:
            pass
        return None

    async def _download_via_curl(self, url: str) -> bytes | None:
        try:
            result = subprocess.run(
                [
                    "curl", "-sL", "--compressed",
                    "-H", f"User-Agent: {_BROWSER_UA}",
                    "-H", "Accept: application/pdf,*/*",
                    url,
                ],
                capture_output=True,
                timeout=120,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except FileNotFoundError:
            pass
        return None

    async def _download_via_httpx(self, url: str) -> bytes | None:
        async with httpx.AsyncClient(
            timeout=120.0,
            follow_redirects=True,
            headers={"User-Agent": _BROWSER_UA, "Accept": "*/*"},
        ) as client:
            response = await client.get(url)
            if response.status_code == 200:
                return response.content
        return None

    def _extract_bts_data(
        self, pdf_path: Path, report_year: int,
    ) -> list[DataPoint]:
        """Extract port ranking tables from a BTS annual report PDF."""
        data_points: list[DataPoint] = []

        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                full_text += page_text + "\n\n"

        data_year = report_year - 1
        ts = datetime(data_year, 12, 31, tzinfo=UTC)

        tonnage = self._extract_ranking_table(
            full_text,
            r"Top 25 U\.S\. Ports by Total Tonnage",
        )
        for rank, port, value in tonnage:
            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id=f"BTS_TONNAGE_{self._port_key(port)}",
                    timestamp=ts,
                    value=value,
                    unit="short_tons",
                    metadata={
                        "port": port,
                        "rank": rank,
                        "category": "total_tonnage",
                        "data_year": data_year,
                        "report_year": report_year,
                        "source": "bts_pdf",
                    },
                )
            )

        dry_bulk = self._extract_ranking_table(
            full_text,
            r"Top 25 Ports by Dry Bulk Tonnage",
        )
        for rank, port, value in dry_bulk:
            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id=f"BTS_DRY_BULK_{self._port_key(port)}",
                    timestamp=ts,
                    value=value,
                    unit="short_tons",
                    metadata={
                        "port": port,
                        "rank": rank,
                        "category": "dry_bulk",
                        "data_year": data_year,
                        "report_year": report_year,
                        "source": "bts_pdf",
                    },
                )
            )

        teu = self._extract_ranking_table(
            full_text,
            r"Top 25 Ports by Loaded TEUs",
        )
        for rank, port, value in teu:
            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id=f"BTS_TEU_{self._port_key(port)}",
                    timestamp=ts,
                    value=value,
                    unit="TEU",
                    metadata={
                        "port": port,
                        "rank": rank,
                        "category": "loaded_teu",
                        "data_year": data_year,
                        "report_year": report_year,
                        "source": "bts_pdf",
                    },
                )
            )

        self.logger.info(
            "Extracted BTS data",
            report_year=report_year,
            data_year=data_year,
            tonnage_ports=len(tonnage),
            dry_bulk_ports=len(dry_bulk),
            teu_ports=len(teu),
            total_points=len(data_points),
        )
        return data_points

    @staticmethod
    def _extract_ranking_table(
        text: str,
        table_header_pattern: str,
        max_entries: int = 25,
    ) -> list[tuple[int, str, float]]:
        """Extract ranked port data from the PDF text.

        Skips TOC entries (which contain ``...`` page refs) and finds the
        actual data table.  Values are stored as-is from the PDF.
        """
        for m in re.finditer(table_header_pattern, text, re.IGNORECASE):
            after = text[m.end(): m.end() + 200]
            if "..." in after or "…" in after:
                continue

            after_header = text[m.end():]
            lines = after_header.split("\n")

            results: list[tuple[int, str, float]] = []
            started = False

            for line in lines:
                line = line.strip()
                if not line:
                    if started and len(results) >= max_entries:
                        break
                    continue

                row_m = _RANKING_TABLE_RE.match(line)
                if row_m:
                    rank = int(row_m.group(1))
                    if rank == 0:
                        continue
                    started = True
                    port = row_m.group(2).strip().rstrip(",")
                    val_str = row_m.group(3).replace(",", "")
                    try:
                        value = float(val_str)
                    except ValueError:
                        continue

                    results.append((rank, port, value))
                    if len(results) >= max_entries:
                        break

                elif started and not any(c.isdigit() for c in line):
                    if len(results) >= 10:
                        break

            if results:
                return results

        return []

    @staticmethod
    def _port_key(port_name: str) -> str:
        """Convert a port name to a stable series key."""
        key = port_name.upper()
        key = re.sub(r"[^A-Z0-9]+", "_", key)
        key = key.strip("_")
        if len(key) > 40:
            key = key[:40].rstrip("_")
        return key

    @staticmethod
    def _find_date_column(df: pd.DataFrame) -> str | None:
        if "Date" in df.columns:
            return "Date"
        for col in df.columns:
            cl = col.lower()
            if cl in ("date", "period", "month-year"):
                return col
        for col in df.columns:
            cl = col.lower()
            if "date" in cl or "period" in cl:
                return col
        return None

    @staticmethod
    def _map_value_columns(df: pd.DataFrame) -> dict[str, str]:
        mapped: dict[str, str] = {}
        for col_name, suffix in POLA_COLUMN_MAP.items():
            if col_name in df.columns:
                mapped[suffix] = col_name
        if mapped:
            return mapped

        for col in df.columns:
            cl = col.lower()
            if "teu" not in cl and "change" not in cl:
                continue
            try:
                pd.to_numeric(df[col], errors="raise")
            except (ValueError, TypeError):
                continue
            safe = col.upper().replace(" ", "_").replace("%", "PCT")
            mapped[safe] = col
        return mapped

    @staticmethod
    def _parse_row_timestamp(
        row: pd.Series, date_col: str,
    ) -> datetime | None:
        raw = row.get(date_col)
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return None
        try:
            ts = pd.to_datetime(raw)
            if ts.tzinfo is None:
                ts = ts.tz_localize(UTC)
            return ts.to_pydatetime()
        except Exception:
            return None


class PortMetricsAggregator:
    """Helper class for aggregating port metrics."""

    @staticmethod
    def calculate_yoy_change(current: float, prior_year: float) -> float:
        if prior_year == 0:
            return 0.0
        return (current - prior_year) / prior_year

    @staticmethod
    def calculate_mom_change(current: float, prior_month: float) -> float:
        if prior_month == 0:
            return 0.0
        return (current - prior_month) / prior_month

    @staticmethod
    def calculate_3m_average(values: list[float]) -> float:
        if len(values) < 3:
            return sum(values) / len(values) if values else 0.0
        return sum(values[:3]) / 3


def get_shipping_collector() -> ShippingDataCollector:
    """Get a Shipping Data collector instance."""
    return ShippingDataCollector()
