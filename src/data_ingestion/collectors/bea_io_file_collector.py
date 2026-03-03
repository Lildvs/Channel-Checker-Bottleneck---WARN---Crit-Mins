"""BEA Input-Output Tables file-based collector.

Provides a fallback mechanism for downloading I-O tables as CSV/Excel files
from BEA's Interactive Tables application when the API is unavailable.

BEA I-O data is available at:
- Interactive Tables: https://apps.bea.gov/iTable/
- Direct data downloads: https://apps.bea.gov/iTable/iTable.cfm

This collector is designed to be used as a fallback when the main API
collector fails due to rate limiting, timeout, or API issues.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import DataFrequency, DataPoint
from src.data_ingestion.file_collector import (
    DatasetConfig,
    FileBasedCollector,
    FileFormat,
)

logger = structlog.get_logger()


BEA_ITABLE_BASE = "https://apps.bea.gov/iTable/"

# Known download URLs for I-O tables (these may change with BEA updates)
BEA_IO_DOWNLOAD_URLS: dict[str, tuple[str, FileFormat, str]] = {
    "total_req_sector": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=56",
        FileFormat.CSV,
        "Industry-by-Commodity Total Requirements (Sector)",
    ),
    "total_req_summary": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=57",
        FileFormat.CSV,
        "Industry-by-Commodity Total Requirements (Summary)",
    ),
    "direct_req_sector": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=54",
        FileFormat.CSV,
        "Industry-by-Commodity Direct Requirements (Sector)",
    ),
    "direct_req_summary": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=55",
        FileFormat.CSV,
        "Industry-by-Commodity Direct Requirements (Summary)",
    ),
    # Make tables
    "make_sector": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=46",
        FileFormat.CSV,
        "Make Table (Sector)",
    ),
    "make_summary": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=47",
        FileFormat.CSV,
        "Make Table (Summary)",
    ),
    "use_sector": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=48",
        FileFormat.CSV,
        "Use Table (Sector)",
    ),
    "use_summary": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=49",
        FileFormat.CSV,
        "Use Table (Summary)",
    ),
    "supply_sector": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=50",
        FileFormat.CSV,
        "Supply Table (Sector)",
    ),
    "supply_summary": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=51",
        FileFormat.CSV,
        "Supply Table (Summary)",
    ),
    "import_matrix_sector": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=52",
        FileFormat.CSV,
        "Import Matrix (Sector)",
    ),
    "import_matrix_summary": (
        "https://apps.bea.gov/iTable/iTable.cfm?reqid=150&step=2&isuri=1&15010=53",
        FileFormat.CSV,
        "Import Matrix (Summary)",
    ),
}


@dataclass
class IOTableFileConfig:
    """Configuration for an I-O table file download."""

    table_key: str
    table_type: str
    detail_level: str
    url: str
    file_format: FileFormat
    description: str


IO_FILE_CONFIGS: dict[str, IOTableFileConfig] = {
    "total_req_sector": IOTableFileConfig(
        table_key="total_req_sector",
        table_type="total_requirements",
        detail_level="sector",
        url=BEA_IO_DOWNLOAD_URLS["total_req_sector"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["total_req_sector"][1],
        description=BEA_IO_DOWNLOAD_URLS["total_req_sector"][2],
    ),
    "total_req_summary": IOTableFileConfig(
        table_key="total_req_summary",
        table_type="total_requirements",
        detail_level="summary",
        url=BEA_IO_DOWNLOAD_URLS["total_req_summary"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["total_req_summary"][1],
        description=BEA_IO_DOWNLOAD_URLS["total_req_summary"][2],
    ),
    "direct_req_sector": IOTableFileConfig(
        table_key="direct_req_sector",
        table_type="direct_requirements",
        detail_level="sector",
        url=BEA_IO_DOWNLOAD_URLS["direct_req_sector"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["direct_req_sector"][1],
        description=BEA_IO_DOWNLOAD_URLS["direct_req_sector"][2],
    ),
    "direct_req_summary": IOTableFileConfig(
        table_key="direct_req_summary",
        table_type="direct_requirements",
        detail_level="summary",
        url=BEA_IO_DOWNLOAD_URLS["direct_req_summary"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["direct_req_summary"][1],
        description=BEA_IO_DOWNLOAD_URLS["direct_req_summary"][2],
    ),
    "make_sector": IOTableFileConfig(
        table_key="make_sector",
        table_type="make",
        detail_level="sector",
        url=BEA_IO_DOWNLOAD_URLS["make_sector"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["make_sector"][1],
        description=BEA_IO_DOWNLOAD_URLS["make_sector"][2],
    ),
    "make_summary": IOTableFileConfig(
        table_key="make_summary",
        table_type="make",
        detail_level="summary",
        url=BEA_IO_DOWNLOAD_URLS["make_summary"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["make_summary"][1],
        description=BEA_IO_DOWNLOAD_URLS["make_summary"][2],
    ),
    "use_sector": IOTableFileConfig(
        table_key="use_sector",
        table_type="use",
        detail_level="sector",
        url=BEA_IO_DOWNLOAD_URLS["use_sector"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["use_sector"][1],
        description=BEA_IO_DOWNLOAD_URLS["use_sector"][2],
    ),
    "use_summary": IOTableFileConfig(
        table_key="use_summary",
        table_type="use",
        detail_level="summary",
        url=BEA_IO_DOWNLOAD_URLS["use_summary"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["use_summary"][1],
        description=BEA_IO_DOWNLOAD_URLS["use_summary"][2],
    ),
    "supply_sector": IOTableFileConfig(
        table_key="supply_sector",
        table_type="supply",
        detail_level="sector",
        url=BEA_IO_DOWNLOAD_URLS["supply_sector"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["supply_sector"][1],
        description=BEA_IO_DOWNLOAD_URLS["supply_sector"][2],
    ),
    "supply_summary": IOTableFileConfig(
        table_key="supply_summary",
        table_type="supply",
        detail_level="summary",
        url=BEA_IO_DOWNLOAD_URLS["supply_summary"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["supply_summary"][1],
        description=BEA_IO_DOWNLOAD_URLS["supply_summary"][2],
    ),
    "import_matrix_sector": IOTableFileConfig(
        table_key="import_matrix_sector",
        table_type="import_matrix",
        detail_level="sector",
        url=BEA_IO_DOWNLOAD_URLS["import_matrix_sector"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["import_matrix_sector"][1],
        description=BEA_IO_DOWNLOAD_URLS["import_matrix_sector"][2],
    ),
    "import_matrix_summary": IOTableFileConfig(
        table_key="import_matrix_summary",
        table_type="import_matrix",
        detail_level="summary",
        url=BEA_IO_DOWNLOAD_URLS["import_matrix_summary"][0],
        file_format=BEA_IO_DOWNLOAD_URLS["import_matrix_summary"][1],
        description=BEA_IO_DOWNLOAD_URLS["import_matrix_summary"][2],
    ),
}


class BEAIOFileCollector(FileBasedCollector):
    """File-based collector for BEA Input-Output Tables.

    Downloads I-O tables as CSV/Excel files from BEA's Interactive Tables
    application. This is used as a fallback when the main API collector fails.

    The collector:
    1. Downloads files from BEA's iTable application
    2. Parses the matrix-format CSV/Excel files
    3. Extracts industry-to-industry coefficients
    4. Stores to the io_coefficients database table
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        table_keys: list[str] | None = None,
    ):
        """Initialize the file-based BEA I-O collector.

        Args:
            data_dir: Base directory for storing downloaded files
            table_keys: Optional list of table keys to collect (defaults to all)
        """
        super().__init__(
            name="BEA I-O Tables (File)",
            source_id="bea_io_file",
            data_dir=data_dir,
            timeout=120.0,  # BEA tables can be large
            max_retries=3,
        )
        self._table_keys = table_keys or list(IO_FILE_CONFIGS.keys())

    @property
    def frequency(self) -> DataFrequency:
        """I-O tables are updated annually."""
        return DataFrequency.ANNUAL

    def get_schedule(self) -> str:
        """Run annually on October 1st (after September release)."""
        return "0 14 1 10 *"

    def get_datasets(self) -> list[DatasetConfig]:
        """Get the list of I-O table datasets to collect."""
        datasets = []
        for key in self._table_keys:
            config = IO_FILE_CONFIGS.get(key)
            if config:
                datasets.append(
                    DatasetConfig(
                        dataset_id=config.table_key,
                        url=config.url,
                        format=config.file_format,
                        filename=f"bea_io_{config.table_key}.csv",
                        description=config.description,
                        expected_frequency=DataFrequency.ANNUAL,
                    )
                )
        return datasets

    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        """Convert a parsed I-O table DataFrame to DataPoints.

        BEA I-O table CSV format is typically a matrix with:
        - First row: column headers (industry codes or names)
        - First column: row headers (industry codes or names)
        - Body: coefficient values

        Args:
            df: Parsed pandas DataFrame from CSV/Excel
            dataset_id: The table key (e.g., 'total_req_summary')

        Returns:
            List of DataPoint objects representing each coefficient
        """
        config = IO_FILE_CONFIGS.get(dataset_id)
        if not config:
            self.logger.warning("Unknown dataset ID", dataset_id=dataset_id)
            return []

        data_points = []
        current_year = datetime.now().year

        try:
            # BEA tables often have metadata rows at the top
            # Look for the actual data matrix
            df = self._clean_bea_dataframe(df)

            if df.empty:
                self.logger.warning("Empty DataFrame after cleaning", dataset_id=dataset_id)
                return []

            row_col = df.columns[0]
            data_columns = df.columns[1:]

            for idx, row in df.iterrows():
                from_industry = str(row[row_col]).strip()
                if not from_industry or from_industry.lower() in ("nan", "total", ""):
                    continue

                for col in data_columns:
                    to_industry = str(col).strip()
                    if not to_industry or to_industry.lower() in ("nan", "total", ""):
                        continue

                    value = row[col]

                    # Skip empty or non-numeric values
                    try:
                        if pd.isna(value):
                            continue
                        value_str = str(value).replace(",", "").strip()
                        if value_str in ("", "---", "n.a.", "n/a", "N/A", "(D)", "(S)"):
                            continue
                        coefficient = float(value_str)
                    except (ValueError, TypeError):
                        continue

                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=f"{dataset_id}_{from_industry}_{to_industry}",
                            timestamp=datetime(current_year - 1, 12, 31),  # Use previous year
                            value=coefficient,
                            metadata={
                                "table_type": config.table_type,
                                "detail_level": config.detail_level,
                                "from_industry": from_industry,
                                "to_industry": to_industry,
                                "year": current_year - 1,
                            },
                        )
                    )

            self.logger.info(
                "Parsed I-O table coefficients",
                dataset_id=dataset_id,
                count=len(data_points),
            )

        except Exception as e:
            self.logger.error(
                "Failed to parse I-O table DataFrame",
                dataset_id=dataset_id,
                error=str(e),
            )

        return data_points

    def _clean_bea_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean a BEA I-O table DataFrame.

        BEA CSV/Excel files often have:
        - Title rows at the top
        - Notes at the bottom
        - Extra columns with footnotes

        Args:
            df: Raw DataFrame from BEA file

        Returns:
            Cleaned DataFrame with just the data matrix
        """
        if df.empty:
            return df

        df = df.dropna(how="all")

        # Find the header row (usually contains industry codes)
        header_row_idx = None
        for idx, row in df.iterrows():
            # Check if row looks like industry codes (starts with numbers or specific patterns)
            first_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
            second_val = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ""

            # BEA industry codes are typically numeric or alphanumeric like "11", "21", "31G"
            if (
                first_val
                and second_val
                and (second_val[0].isdigit() or second_val in ("11", "21", "22", "23", "31G", "42"))
            ):
                header_row_idx = idx
                break

        if header_row_idx is not None and header_row_idx > 0:
            # Set the header row and skip previous rows
            df.columns = df.iloc[header_row_idx]
            df = df.iloc[header_row_idx + 1 :].reset_index(drop=True)

        df = df.dropna(axis=1, how="all")

        valid_cols = []
        for col in df.columns:
            col_str = str(col).strip() if pd.notna(col) else ""
            if col_str and not col_str.startswith("Note") and col_str.lower() != "nan":
                valid_cols.append(col)

        df = df[valid_cols]

        return df

    async def store_coefficients(
        self,
        data_points: list[DataPoint],
    ) -> int:
        """Store extracted coefficients to the database.

        Args:
            data_points: List of DataPoints containing coefficient data

        Returns:
            Number of records stored
        """
        from src.storage.models import IOCoefficient as IOCoefficientModel
        from src.storage.timescale import get_db
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        if not data_points:
            return 0

        db = get_db()
        records = []

        for dp in data_points:
            meta = dp.metadata
            records.append(
                {
                    "year": meta.get("year", datetime.now().year - 1),
                    "table_type": meta.get("table_type", "unknown"),
                    "detail_level": meta.get("detail_level", "unknown"),
                    "from_industry": meta.get("from_industry", ""),
                    "from_industry_name": None,
                    "to_industry": meta.get("to_industry", ""),
                    "to_industry_name": None,
                    "coefficient": Decimal(str(dp.value)) if dp.value is not None else Decimal("0"),
                    "commodity_code": None,
                    "commodity_name": None,
                    "collected_at": datetime.now(UTC),
                }
            )

        try:
            async with db.session() as session:
                stmt = pg_insert(IOCoefficientModel).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "year",
                        "table_type",
                        "detail_level",
                        "from_industry",
                        "to_industry",
                    ],
                    set_={
                        "coefficient": stmt.excluded.coefficient,
                        "collected_at": stmt.excluded.collected_at,
                    },
                )

                await session.execute(stmt)
                await session.commit()

            self.logger.info(
                "Stored I-O coefficients from file download",
                count=len(records),
            )
            return len(records)

        except Exception as e:
            self.logger.error(
                "Failed to store I-O coefficients",
                error=str(e),
            )
            raise


def get_bea_io_file_collector(
    data_dir: Path | None = None,
    table_keys: list[str] | None = None,
) -> BEAIOFileCollector:
    """Factory function to create a BEAIOFileCollector instance.

    Args:
        data_dir: Base directory for storing downloaded files
        table_keys: Optional list of table keys to collect

    Returns:
        Configured BEAIOFileCollector instance
    """
    return BEAIOFileCollector(
        data_dir=data_dir,
        table_keys=table_keys,
    )
