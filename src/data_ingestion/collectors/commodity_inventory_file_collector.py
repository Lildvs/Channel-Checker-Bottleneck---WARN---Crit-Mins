"""File-based commodity inventory collector for LME and COMEX data.

Collects metal warehouse stock data from:
- LME: London Metal Exchange warehouse stocks (Excel, 2-day delayed)
- COMEX: CME Group precious metals registered stocks

Data Sources:
- LME Stocks Summary: https://www.lme.com/en/Market-data/Reports-and-data/Warehouse-and-stocks-reports/Stocks-summary
- LME Stocks Breakdown: https://www.lme.com/en/Market-data/Reports-and-data/Warehouse-and-stocks-reports/Stock-breakdown-report
- CME Metals Reports: https://www.cmegroup.com/clearing/operations-and-deliveries/registrar-reports.html

Note: LME data is free with 2-day delay. CME data requires registration but is free.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from src.data_ingestion.base_collector import DataFrequency, DataPoint
from src.data_ingestion.file_collector import (
    DatasetConfig,
    FileBasedCollector,
    FileFormat,
)

logger = structlog.get_logger()


LME_STOCKS_SUMMARY_URL = "https://www.lme.com/-/media/Files/Market-data/Reports-and-data/Warehouse-and-stocks/Stocks-summary"
LME_STOCKS_BREAKDOWN_URL = "https://www.lme.com/-/media/Files/Market-data/Reports-and-data/Warehouse-and-stocks/Stocks-breakdown"

LME_METALS: dict[str, dict[str, Any]] = {
    "copper": {
        "lme_code": "CA",
        "name": "Copper",
        "unit": "metric_tons",
        "column_pattern": r"copper|CA",
    },
    "aluminum": {
        "lme_code": "AH",
        "name": "Aluminium",
        "unit": "metric_tons",
        "column_pattern": r"aluminium|aluminum|AH",
    },
    "zinc": {
        "lme_code": "ZS",
        "name": "Zinc",
        "unit": "metric_tons",
        "column_pattern": r"zinc|ZS",
    },
    "nickel": {
        "lme_code": "NI",
        "name": "Nickel",
        "unit": "metric_tons",
        "column_pattern": r"nickel|NI",
    },
    "lead": {
        "lme_code": "PB",
        "name": "Lead",
        "unit": "metric_tons",
        "column_pattern": r"lead|PB",
    },
    "tin": {
        "lme_code": "SN",
        "name": "Tin",
        "unit": "metric_tons",
        "column_pattern": r"tin|SN",
    },
    "cobalt": {
        "lme_code": "CO",
        "name": "Cobalt",
        "unit": "metric_tons",
        "column_pattern": r"cobalt|CO",
    },
}


CME_METALS: dict[str, dict[str, Any]] = {
    "gold": {
        "cme_code": "GC",
        "name": "Gold",
        "unit": "troy_ounces",
        "comex": True,
    },
    "silver": {
        "cme_code": "SI",
        "name": "Silver",
        "unit": "troy_ounces",
        "comex": True,
    },
    "copper_comex": {
        "cme_code": "HG",
        "name": "Copper (COMEX)",
        "unit": "pounds",
        "comex": True,
    },
    "platinum": {
        "cme_code": "PL",
        "name": "Platinum",
        "unit": "troy_ounces",
        "comex": False,  # NYMEX
    },
    "palladium": {
        "cme_code": "PA",
        "name": "Palladium",
        "unit": "troy_ounces",
        "comex": False,  # NYMEX
    },
}


@dataclass
class MetalInventoryConfig:
    """Configuration for a metal inventory dataset."""

    dataset_id: str
    source: str  # LME or COMEX
    metal: str
    metal_name: str
    unit: str
    url: str
    file_format: FileFormat
    description: str
    delay_days: int = 2


def _build_lme_configs() -> dict[str, MetalInventoryConfig]:
    """Build LME dataset configurations."""
    configs = {}
    current_year = datetime.now().year

    for metal, info in LME_METALS.items():
        config_key = f"lme_{metal}"
        configs[config_key] = MetalInventoryConfig(
            dataset_id=config_key,
            source="LME",
            metal=metal,
            metal_name=info["name"],
            unit=info["unit"],
            url=f"{LME_STOCKS_SUMMARY_URL}/{current_year}/",
            file_format=FileFormat.EXCEL,
            description=f"LME {info['name']} Warehouse Stocks",
            delay_days=2,
        )

    return configs


def _build_cme_configs() -> dict[str, MetalInventoryConfig]:
    """Build CME/COMEX dataset configurations."""
    configs = {}

    for metal, info in CME_METALS.items():
        config_key = f"comex_{metal}"
        configs[config_key] = MetalInventoryConfig(
            dataset_id=config_key,
            source="COMEX",
            metal=metal,
            metal_name=info["name"],
            unit=info["unit"],
            # CME Registrar Reports URL (actual URL depends on report availability)
            url="https://www.cmegroup.com/clearing/operations-and-deliveries/registrar-reports.html",
            file_format=FileFormat.EXCEL,
            description=f"COMEX {info['name']} Registered Stocks",
            delay_days=1,
        )

    return configs


LME_CONFIGS = _build_lme_configs()
CME_CONFIGS = _build_cme_configs()
ALL_METAL_CONFIGS = {**LME_CONFIGS, **CME_CONFIGS}


class CommodityInventoryFileCollector(FileBasedCollector):
    """File-based collector for LME and COMEX metal warehouse stocks.

    Downloads Excel/CSV reports from exchange websites and parses
    warehouse inventory data for base and precious metals.

    LME Data:
    - Free with 2-day delay
    - Monthly summary reports available as Excel
    - Daily breakdown reports available with subscription

    COMEX Data:
    - Daily registrar reports
    - Registered and eligible stocks for gold, silver, copper
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        sources: list[str] | None = None,
    ):
        """Initialize the metal inventory file collector.

        Args:
            data_dir: Base directory for storing downloaded files
            sources: List of sources to collect from ('lme', 'comex', or both)
        """
        super().__init__(
            name="Metal Inventory (File)",
            source_id="metal_inventory_file",
            data_dir=data_dir,
            timeout=120.0,
            max_retries=3,
        )
        self._sources = sources or ["lme", "comex"]

    @property
    def frequency(self) -> DataFrequency:
        """Data is updated daily but with delays."""
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        """Run daily at 11:00 UTC (after LME morning updates)."""
        return "0 11 * * *"

    def get_datasets(self) -> list[DatasetConfig]:
        """Get the list of metal inventory datasets to collect."""
        datasets = []

        for config_key, config in ALL_METAL_CONFIGS.items():
            source_lower = config.source.lower()
            if source_lower not in [s.lower() for s in self._sources]:
                continue

            # Build the actual download URL
            # Note: LME/CME URLs can change; this is a best-effort approach
            url = self._get_download_url(config)

            datasets.append(
                DatasetConfig(
                    dataset_id=config.dataset_id,
                    url=url,
                    format=config.file_format,
                    filename=f"{config.dataset_id}_{datetime.now().strftime('%Y%m')}.xlsx",
                    description=config.description,
                    expected_frequency=DataFrequency.DAILY,
                )
            )

        return datasets

    def _get_download_url(self, config: MetalInventoryConfig) -> str:
        """Build the actual download URL for a dataset.

        Args:
            config: Metal inventory configuration

        Returns:
            Download URL string
        """
        now = datetime.now()

        if config.source == "LME":
            return (
                f"{LME_STOCKS_SUMMARY_URL}/{now.year}/"
                f"Stocks-summary-{now.strftime('%Y%m')}.xlsx"
            )
        elif config.source == "COMEX":
            return config.url
        else:
            return config.url

    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        """Convert a parsed metal inventory DataFrame to DataPoints.

        Args:
            df: Parsed pandas DataFrame from Excel/CSV
            dataset_id: The dataset identifier (e.g., 'lme_copper')

        Returns:
            List of DataPoint objects
        """
        config = ALL_METAL_CONFIGS.get(dataset_id)
        if not config:
            self.logger.warning("Unknown dataset ID", dataset_id=dataset_id)
            return []

        if config.source == "LME":
            return self._parse_lme_data(df, config)
        elif config.source == "COMEX":
            return self._parse_comex_data(df, config)
        else:
            self.logger.warning("Unknown source", source=config.source)
            return []

    def _parse_lme_data(
        self,
        df: pd.DataFrame,
        config: MetalInventoryConfig,
    ) -> list[DataPoint]:
        """Parse LME stock summary data.

        LME Excel files typically have:
        - Rows for different dates or locations
        - Columns for different metrics (stocks, change, etc.)

        Args:
            df: DataFrame from LME Excel file
            config: Metal configuration

        Returns:
            List of DataPoint objects
        """
        data_points = []

        try:
            df = self._clean_lme_dataframe(df, config)

            if df.empty:
                self.logger.warning(
                    "Empty DataFrame after cleaning",
                    dataset_id=config.dataset_id,
                )
                return []

            stock_cols = [
                col for col in df.columns
                if any(
                    pattern in str(col).lower()
                    for pattern in ["stock", "closing", "total", "tonnage"]
                )
            ]

            for idx, row in df.iterrows():
                timestamp = self._extract_timestamp(row, df, idx)
                if timestamp is None:
                    continue

                for col in stock_cols:
                    try:
                        value = row[col]
                        if pd.isna(value):
                            continue

                        if isinstance(value, str):
                            value = float(value.replace(",", "").strip())
                        else:
                            value = float(value)

                        data_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=f"LME_{config.metal.upper()}_STOCKS",
                                timestamp=timestamp,
                                value=value,
                                unit=config.unit,
                                metadata={
                                    "commodity": config.metal,
                                    "commodity_type": "metal",
                                    "source": "LME",
                                    "stock_type": "warehouse",
                                    "metal_name": config.metal_name,
                                    "column": str(col),
                                    "delay_days": config.delay_days,
                                },
                            )
                        )
                        break  # Only take first valid stock value

                    except (ValueError, TypeError) as e:
                        self.logger.debug(
                            "Failed to parse LME value",
                            column=col,
                            error=str(e),
                        )
                        continue

        except Exception as e:
            self.logger.error(
                "Failed to parse LME data",
                dataset_id=config.dataset_id,
                error=str(e),
            )

        return data_points

    def _clean_lme_dataframe(
        self,
        df: pd.DataFrame,
        config: MetalInventoryConfig,
    ) -> pd.DataFrame:
        """Clean LME DataFrame for parsing.

        Args:
            df: Raw DataFrame
            config: Metal configuration

        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df

        df = df.dropna(how="all")

        # LME files often have title rows before the actual data
        header_row_idx = None
        for idx, row in df.iterrows():
            row_str = " ".join(str(v).lower() for v in row if pd.notna(v))
            if any(term in row_str for term in ["date", "stock", "metal", "tonnage"]):
                header_row_idx = idx
                break

        if header_row_idx is not None and header_row_idx > 0:
            df.columns = df.iloc[header_row_idx]
            df = df.iloc[header_row_idx + 1:].reset_index(drop=True)

        metal_pattern = LME_METALS[config.metal].get("column_pattern", config.metal)
        metal_cols = [
            col for col in df.columns
            if pd.notna(col) and re.search(metal_pattern, str(col), re.IGNORECASE)
        ]

        if metal_cols:
            keep_cols = [df.columns[0]] + metal_cols
            df = df[[col for col in keep_cols if col in df.columns]]

        return df

    def _parse_comex_data(
        self,
        df: pd.DataFrame,
        config: MetalInventoryConfig,
    ) -> list[DataPoint]:
        """Parse COMEX registrar report data.

        COMEX reports typically show:
        - Registered stocks (available for delivery)
        - Eligible stocks (meet quality specs but not registered)

        Args:
            df: DataFrame from COMEX Excel file
            config: Metal configuration

        Returns:
            List of DataPoint objects
        """
        data_points = []

        try:
            df = df.dropna(how="all")

            if df.empty:
                self.logger.warning(
                    "Empty DataFrame",
                    dataset_id=config.dataset_id,
                )
                return []

            for idx, row in df.iterrows():
                timestamp = self._extract_timestamp(row, df, idx)
                if timestamp is None:
                    continue

                for col in df.columns:
                    col_lower = str(col).lower()
                    if "registered" in col_lower or "eligible" in col_lower:
                        try:
                            value = row[col]
                            if pd.isna(value):
                                continue

                            if isinstance(value, str):
                                value = float(value.replace(",", "").strip())
                            else:
                                value = float(value)

                            stock_type = "registered" if "registered" in col_lower else "eligible"

                            data_points.append(
                                DataPoint(
                                    source_id=self.source_id,
                                    series_id=f"COMEX_{config.metal.upper()}_{stock_type.upper()}",
                                    timestamp=timestamp,
                                    value=value,
                                    unit=config.unit,
                                    metadata={
                                        "commodity": config.metal,
                                        "commodity_type": "metal",
                                        "source": "COMEX",
                                        "stock_type": stock_type,
                                        "metal_name": config.metal_name,
                                        "delay_days": config.delay_days,
                                    },
                                )
                            )

                        except (ValueError, TypeError) as e:
                            self.logger.debug(
                                "Failed to parse COMEX value",
                                column=col,
                                error=str(e),
                            )
                            continue

        except Exception as e:
            self.logger.error(
                "Failed to parse COMEX data",
                dataset_id=config.dataset_id,
                error=str(e),
            )

        return data_points

    def _extract_timestamp(
        self,
        row: pd.Series,
        df: pd.DataFrame,
        idx: Any,
    ) -> datetime | None:
        """Extract timestamp from a row.

        Args:
            row: DataFrame row
            df: Full DataFrame
            idx: Row index

        Returns:
            datetime or None
        """
        first_val = row.iloc[0] if len(row) > 0 else None

        if first_val is not None:
            try:
                if isinstance(first_val, datetime):
                    return first_val
                elif isinstance(first_val, pd.Timestamp):
                    return first_val.to_pydatetime()
                elif isinstance(first_val, str):
                    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%d-%m-%Y"]:
                        try:
                            return datetime.strptime(first_val.strip(), fmt)
                        except ValueError:
                            continue
            except Exception:
                pass

        if isinstance(df.index, pd.DatetimeIndex):
            try:
                return df.index[idx].to_pydatetime()
            except Exception:
                pass

        return datetime.now()

    async def collect_lme_stocks(self) -> list[DataPoint]:
        """Collect only LME metal stocks.

        Returns:
            List of DataPoint objects for LME metals
        """
        original_sources = self._sources
        self._sources = ["lme"]
        try:
            return await self.collect()
        finally:
            self._sources = original_sources

    async def collect_comex_stocks(self) -> list[DataPoint]:
        """Collect only COMEX metal stocks.

        Returns:
            List of DataPoint objects for COMEX metals
        """
        original_sources = self._sources
        self._sources = ["comex"]
        try:
            return await self.collect()
        finally:
            self._sources = original_sources

    async def store_inventory(
        self,
        data_points: list[DataPoint],
    ) -> int:
        """Store collected inventory data to the database.

        Args:
            data_points: List of DataPoints containing inventory data

        Returns:
            Number of records stored
        """
        from src.storage.models import CommodityInventory
        from src.storage.timescale import get_db
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        if not data_points:
            return 0

        db = get_db()
        records = []

        for dp in data_points:
            meta = dp.metadata or {}
            records.append(
                {
                    "commodity": meta.get("commodity", "unknown"),
                    "commodity_type": meta.get("commodity_type", "metal"),
                    "source": meta.get("source", "unknown"),
                    "location": meta.get("location"),
                    "quantity": Decimal(str(dp.value)) if dp.value is not None else Decimal("0"),
                    "unit": dp.unit or meta.get("unit", "unknown"),
                    "stock_type": meta.get("stock_type"),
                    "timestamp": dp.timestamp,
                    "data_delay_days": meta.get("delay_days", 0),
                    "reporting_period": meta.get("reporting_period"),
                    "collected_at": datetime.now(UTC),
                    "raw_metadata": meta,
                }
            )

        try:
            async with db.session() as session:
                stmt = pg_insert(CommodityInventory).values(records)
                # On conflict, update the quantity and timestamp
                stmt = stmt.on_conflict_do_nothing()  # Skip duplicates

                await session.execute(stmt)
                await session.commit()

            self.logger.info(
                "Stored metal inventory data",
                count=len(records),
            )
            return len(records)

        except Exception as e:
            self.logger.error(
                "Failed to store metal inventory",
                error=str(e),
            )
            raise


def get_commodity_inventory_file_collector(
    data_dir: Path | None = None,
    sources: list[str] | None = None,
) -> CommodityInventoryFileCollector:
    """Factory function to create a CommodityInventoryFileCollector.

    Args:
        data_dir: Base directory for storing downloaded files
        sources: List of sources ('lme', 'comex', or both)

    Returns:
        Configured CommodityInventoryFileCollector instance
    """
    return CommodityInventoryFileCollector(
        data_dir=data_dir,
        sources=sources,
    )


def get_lme_collector(data_dir: Path | None = None) -> CommodityInventoryFileCollector:
    """Get a collector configured for LME data only.

    Args:
        data_dir: Base directory for storing downloaded files

    Returns:
        CommodityInventoryFileCollector for LME data
    """
    return CommodityInventoryFileCollector(data_dir=data_dir, sources=["lme"])


def get_comex_collector(data_dir: Path | None = None) -> CommodityInventoryFileCollector:
    """Get a collector configured for COMEX data only.

    Args:
        data_dir: Base directory for storing downloaded files

    Returns:
        CommodityInventoryFileCollector for COMEX data
    """
    return CommodityInventoryFileCollector(data_dir=data_dir, sources=["comex"])
