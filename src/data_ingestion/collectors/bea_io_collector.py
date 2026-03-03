"""BEA Input-Output Tables collector.

Collects inter-industry dependency data from the Bureau of Economic Analysis.
This data is critical for understanding how supply chain disruptions in one
industry propagate to others.

BEA I-O Tables include:
- Make Tables: What each industry produces
- Use Tables: What inputs each industry consumes
- Direct Requirements: Immediate inputs per $1 output
- Total Requirements: Direct + indirect inputs (Leontief inverse)
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import structlog

from src.config.settings import get_settings
from src.config.bea_industry_mapping import (
    get_bea_industry,
    map_bea_to_sector,
    BEA_SUMMARY_INDUSTRIES,
)
from src.data_ingestion.base_collector import (
    BaseCollector,
    CollectionResult,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()


@dataclass
class IOTableConfig:
    """Configuration for an I-O table to fetch."""

    table_id: int
    name: str
    table_type: str  # See IO_TABLE_TYPES for valid values
    detail_level: str  # 'sector' or 'summary'
    description: str
    has_commodity_dimension: bool = False  # True for Make/Use/Supply tables


IO_TABLE_TYPES = frozenset({
    "direct_requirements",
    "total_requirements",
    "make",
    "use",
    "supply",
    "import_matrix",
})


# Table IDs from BEA API - verified via GetParameterValues
# Reference: https://apps.bea.gov/api/data - InputOutput dataset
IO_TABLES: dict[str, IOTableConfig] = {
    "total_req_sector": IOTableConfig(
        table_id=56,
        name="Industry-by-Commodity Total Requirements (Sector)",
        table_type="total_requirements",
        detail_level="sector",
        description="15-industry total requirements after redefinitions",
    ),
    "total_req_summary": IOTableConfig(
        table_id=57,
        name="Industry-by-Commodity Total Requirements (Summary)",
        table_type="total_requirements",
        detail_level="summary",
        description="71-industry total requirements after redefinitions",
    ),
    "direct_req_sector": IOTableConfig(
        table_id=54,
        name="Industry-by-Commodity Direct Requirements (Sector)",
        table_type="direct_requirements",
        detail_level="sector",
        description="15-industry direct requirements after redefinitions",
    ),
    "direct_req_summary": IOTableConfig(
        table_id=55,
        name="Industry-by-Commodity Direct Requirements (Summary)",
        table_type="direct_requirements",
        detail_level="summary",
        description="71-industry direct requirements after redefinitions",
    ),
    "make_sector": IOTableConfig(
        table_id=46,
        name="Make Table (Sector Level)",
        table_type="make",
        detail_level="sector",
        description="15-industry domestic production of commodities",
        has_commodity_dimension=True,
    ),
    "make_summary": IOTableConfig(
        table_id=47,
        name="Make Table (Summary Level)",
        table_type="make",
        detail_level="summary",
        description="71-industry domestic production of commodities",
        has_commodity_dimension=True,
    ),
    "use_sector": IOTableConfig(
        table_id=48,
        name="Use Table (Sector Level)",
        table_type="use",
        detail_level="sector",
        description="15-industry commodity consumption",
        has_commodity_dimension=True,
    ),
    "use_summary": IOTableConfig(
        table_id=49,
        name="Use Table (Summary Level)",
        table_type="use",
        detail_level="summary",
        description="71-industry commodity consumption",
        has_commodity_dimension=True,
    ),
    "supply_sector": IOTableConfig(
        table_id=50,
        name="Supply Table (Sector Level)",
        table_type="supply",
        detail_level="sector",
        description="15-industry total commodity supply",
        has_commodity_dimension=True,
    ),
    "supply_summary": IOTableConfig(
        table_id=51,
        name="Supply Table (Summary Level)",
        table_type="supply",
        detail_level="summary",
        description="71-industry total commodity supply",
        has_commodity_dimension=True,
    ),
    "import_matrix_sector": IOTableConfig(
        table_id=52,
        name="Import Matrix (Sector Level)",
        table_type="import_matrix",
        detail_level="sector",
        description="15-industry imported commodity use",
        has_commodity_dimension=True,
    ),
    "import_matrix_summary": IOTableConfig(
        table_id=53,
        name="Import Matrix (Summary Level)",
        table_type="import_matrix",
        detail_level="summary",
        description="71-industry imported commodity use",
        has_commodity_dimension=True,
    ),
}


@dataclass
class IOCoefficient:
    """Represents a single I-O coefficient.

    For requirements tables (direct/total):
        - from_industry: Source industry providing input
        - to_industry: Consuming industry receiving input
        - coefficient: Value of inputs from source per $ of output

    For Make/Use/Supply tables:
        - from_industry: Industry code (producer)
        - to_industry: Commodity code (product)
        - coefficient: Value of production/consumption
        - commodity_code: Explicit commodity code (may differ from to_industry)
    """

    year: int
    table_type: str
    detail_level: str
    from_industry: str
    from_industry_name: str | None
    to_industry: str
    to_industry_name: str | None
    coefficient: Decimal
    commodity_code: str | None = None
    commodity_name: str | None = None


class BEAIOCollector(BaseCollector):
    """Collector for BEA Input-Output Tables.

    Fetches inter-industry dependency coefficients from BEA's API.
    These coefficients show how much output from one industry is required
    to produce one dollar of output in another industry.
    """

    BASE_URL = "https://apps.bea.gov/api/data"

    def __init__(self):
        """Initialize BEA I-O collector."""
        super().__init__(name="BEA I-O Tables", source_id="bea_io")
        settings = get_settings()
        self.api_key = settings.bea_api_key
        self.rate_limiter = get_rate_limiter("bea")
        self._available_tables: dict[int, str] | None = None
        self._available_years: list[int] | None = None

    @property
    def frequency(self) -> DataFrequency:
        """I-O tables are updated annually (September release)."""
        return DataFrequency.ANNUAL

    def get_schedule(self) -> str:
        """Run annually on October 1st (after September release)."""
        return "0 14 1 10 *"

    def get_default_series(self) -> list[str]:
        """Return default table configurations to collect."""
        return list(IO_TABLES.keys())

    async def validate_api_key(self) -> bool:
        """Validate the BEA API key."""
        if not self.api_key:
            self.logger.warning("BEA API key not configured")
            return False

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    self.BASE_URL,
                    params={
                        "UserID": self.api_key.get_secret_value(),
                        "method": "GetDatasetList",
                        "ResultFormat": "JSON",
                    },
                )
                data = response.json()
                return "BEAAPI" in data and "Results" in data.get("BEAAPI", {})
        except Exception as e:
            self.logger.error("BEA API key validation failed", error=str(e))
            return False

    async def _get_available_tables(
        self, client: httpx.AsyncClient
    ) -> dict[int, str]:
        """Get list of available I-O tables from API.

        Returns:
            Dict of table_id -> table description
        """
        if self._available_tables is not None:
            return self._available_tables

        async with self.rate_limiter:
            response = await client.get(
                self.BASE_URL,
                params={
                    "UserID": self.api_key.get_secret_value(),
                    "method": "GetParameterValues",
                    "DataSetName": "InputOutput",
                    "ParameterName": "TableID",
                    "ResultFormat": "JSON",
                },
            )
            response.raise_for_status()
            data = response.json()

        tables = {}
        param_values = (
            data.get("BEAAPI", {})
            .get("Results", {})
            .get("ParamValue", [])
        )

        for pv in param_values:
            try:
                table_id = int(pv.get("Key", 0))
                description = pv.get("Desc", "")
                tables[table_id] = description
            except (ValueError, TypeError):
                continue

        self._available_tables = tables
        self.logger.info("Retrieved available I-O tables", count=len(tables))
        return tables

    async def _get_available_years(
        self, client: httpx.AsyncClient
    ) -> list[int]:
        """Get list of available years from API.

        Returns:
            List of available years (sorted descending)
        """
        if self._available_years is not None:
            return self._available_years

        async with self.rate_limiter:
            response = await client.get(
                self.BASE_URL,
                params={
                    "UserID": self.api_key.get_secret_value(),
                    "method": "GetParameterValues",
                    "DataSetName": "InputOutput",
                    "ParameterName": "Year",
                    "ResultFormat": "JSON",
                },
            )
            response.raise_for_status()
            data = response.json()

        years = []
        param_values = (
            data.get("BEAAPI", {})
            .get("Results", {})
            .get("ParamValue", [])
        )

        for pv in param_values:
            try:
                year = int(pv.get("Key", 0))
                if year > 1990:  # Reasonable cutoff
                    years.append(year)
            except (ValueError, TypeError):
                continue

        self._available_years = sorted(years, reverse=True)
        self.logger.info(
            "Retrieved available years",
            count=len(self._available_years),
            latest=self._available_years[0] if self._available_years else None,
        )
        return self._available_years

    async def _fetch_io_table(
        self,
        client: httpx.AsyncClient,
        table_id: int,
        year: int,
    ) -> list[dict[str, Any]]:
        """Fetch I-O table data from BEA API.

        Args:
            client: HTTP client
            table_id: BEA table ID
            year: Year to fetch

        Returns:
            List of data rows from API
        """
        async with self.rate_limiter:
            response = await client.get(
                self.BASE_URL,
                params={
                    "UserID": self.api_key.get_secret_value(),
                    "method": "GetData",
                    "DataSetName": "InputOutput",
                    "TableID": str(table_id),
                    "Year": str(year),
                    "ResultFormat": "JSON",
                },
            )
            response.raise_for_status()
            data = response.json()

        results = data.get("BEAAPI", {}).get("Results", {})

        # BEA sometimes returns Results as a list instead of a dict
        if isinstance(results, list):
            if results and isinstance(results[0], dict):
                return results
            return []

        if "Error" in results:
            error = results["Error"]
            raise ValueError(f"BEA API error: {error}")

        return results.get("Data", [])

    def _parse_io_rows(
        self,
        rows: list[dict[str, Any]],
        config: IOTableConfig,
        year: int,
    ) -> list[IOCoefficient]:
        """Parse raw API rows into IOCoefficient objects.

        Args:
            rows: Raw data rows from API
            config: Table configuration
            year: Data year

        Returns:
            List of IOCoefficient objects
        """
        coefficients = []

        for row in rows:
            try:
                # BEA I-O data structure varies by table type:
                #
                # Requirements tables (direct/total):
                #   - RowCode: Source industry (from_industry)
                #   - ColCode: Consuming industry (to_industry)
                #   - DataValue: The coefficient
                #
                # Make/Use/Supply tables:
                #   - RowCode: Industry code or Commodity code
                #   - ColCode: Commodity code or Industry code
                #   - DataValue: Production/consumption value
                #   - May have separate CommodityCode field
                #
                from_industry = row.get("RowCode", "")
                to_industry = row.get("ColCode", "")
                value_str = row.get("DataValue", "0")

                # Skip if missing required data
                if not from_industry or not to_industry:
                    continue

                try:
                    # Handle various formats (commas, parentheses for negatives)
                    value_str = str(value_str).replace(",", "").replace("(", "-").replace(")", "")
                    if value_str in ("", "---", "n.a.", "n/a", "N/A", "(D)", "(S)"):
                        continue
                    coefficient = Decimal(value_str)
                except Exception:
                    continue

                from_name = row.get("RowDescription", None)
                to_name = row.get("ColDescription", None)

                commodity_code: str | None = None
                commodity_name: str | None = None

                if config.has_commodity_dimension:
                    # For Make tables: RowCode=Industry, ColCode=Commodity
                    # For Use tables: RowCode=Commodity, ColCode=Industry
                    # For Supply tables: RowCode=Commodity, ColCode=Industry
                    # For Import Matrix: RowCode=Commodity, ColCode=Industry
                    if config.table_type == "make":
                        commodity_code = to_industry
                        commodity_name = to_name
                    else:
                        # Use, Supply, Import Matrix: commodity is in row
                        commodity_code = from_industry
                        commodity_name = from_name

                coefficients.append(
                    IOCoefficient(
                        year=year,
                        table_type=config.table_type,
                        detail_level=config.detail_level,
                        from_industry=from_industry,
                        from_industry_name=from_name,
                        to_industry=to_industry,
                        to_industry_name=to_name,
                        coefficient=coefficient,
                        commodity_code=commodity_code,
                        commodity_name=commodity_name,
                    )
                )

            except Exception as e:
                self.logger.warning(
                    "Failed to parse I-O row",
                    error=str(e),
                    row=row,
                )
                continue

        return coefficients

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect I-O table data from BEA API.

        Note: For I-O tables, we don't return DataPoints in the traditional sense.
        Instead, the data is stored directly to the io_coefficients table.
        This method returns a summary as DataPoints for consistency with the
        collector interface.

        Args:
            series_ids: Table configuration keys to collect (or all if None)
            start_date: Start year (uses year only)
            end_date: End year (uses year only)

        Returns:
            List of summary DataPoints (one per table/year combination)
        """
        if not self.api_key:
            self.logger.error("BEA API key not configured")
            return []

        table_keys = series_ids or self.get_default_series()
        all_coefficients: list[IOCoefficient] = []
        summary_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=60.0) as client:
            available_years = await self._get_available_years(client)

            if start_date and end_date:
                years = [y for y in available_years 
                        if start_date.year <= y <= end_date.year]
            elif start_date:
                years = [y for y in available_years if y >= start_date.year]
            else:
                # Default: latest 3 years
                years = available_years[:3]

            self.logger.info(
                "Collecting I-O tables",
                tables=table_keys,
                years=years,
            )

            for table_key in table_keys:
                config = IO_TABLES.get(table_key)
                if not config:
                    self.logger.warning("Unknown table key", key=table_key)
                    continue

                for year in years:
                    try:
                        self.logger.debug(
                            "Fetching I-O table",
                            table=config.name,
                            year=year,
                        )

                        rows = await self._fetch_io_table(
                            client, config.table_id, year
                        )

                        coefficients = self._parse_io_rows(rows, config, year)
                        all_coefficients.extend(coefficients)

                        summary_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=f"{table_key}_{year}",
                                timestamp=datetime(year, 12, 31),
                                value=float(len(coefficients)),
                                metadata={
                                    "table_id": config.table_id,
                                    "table_name": config.name,
                                    "table_type": config.table_type,
                                    "detail_level": config.detail_level,
                                    "year": year,
                                    "coefficient_count": len(coefficients),
                                },
                            )
                        )

                        self.logger.info(
                            "Collected I-O table",
                            table=config.name,
                            year=year,
                            coefficients=len(coefficients),
                        )

                    except Exception as e:
                        self.logger.error(
                            "Failed to fetch I-O table",
                            table=config.name,
                            year=year,
                            error=str(e),
                        )

        if all_coefficients:
            await self._store_coefficients(all_coefficients)

        return summary_points

    async def _store_coefficients(
        self, coefficients: list[IOCoefficient]
    ) -> None:
        """Store I-O coefficients to database.

        Args:
            coefficients: List of coefficients to store
        """
        from src.storage.timescale import get_db
        from src.storage.models import IOCoefficient as IOCoefficientModel
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        db = get_db()

        records = [
            {
                "year": c.year,
                "table_type": c.table_type,
                "detail_level": c.detail_level,
                "from_industry": c.from_industry,
                "from_industry_name": c.from_industry_name,
                "to_industry": c.to_industry,
                "to_industry_name": c.to_industry_name,
                "coefficient": c.coefficient,
                "commodity_code": c.commodity_code,
                "commodity_name": c.commodity_name,
                "collected_at": datetime.now(UTC),
            }
            for c in coefficients
        ]

        try:
            async with db.session() as session:
                stmt = pg_insert(IOCoefficientModel).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "year", "table_type", "detail_level",
                        "from_industry", "to_industry"
                    ],
                    set_={
                        "from_industry_name": stmt.excluded.from_industry_name,
                        "to_industry_name": stmt.excluded.to_industry_name,
                        "coefficient": stmt.excluded.coefficient,
                        "commodity_code": stmt.excluded.commodity_code,
                        "commodity_name": stmt.excluded.commodity_name,
                        "collected_at": stmt.excluded.collected_at,
                    },
                )

                await session.execute(stmt)
                await session.commit()

            # Monitor for commodity_code collisions on the unique constraint
            # If the same (year, table_type, detail_level, from_industry, to_industry)
            # appears with different commodity_codes, the upsert silently overwrites.
            # Log a warning so this can be detected and the constraint expanded if needed.
            seen_keys: dict[tuple, list[str | None]] = {}
            for r in records:
                key = (r["year"], r["table_type"], r["detail_level"],
                       r["from_industry"], r["to_industry"])
                commodity = r.get("commodity_code")
                if key in seen_keys:
                    if commodity not in seen_keys[key]:
                        seen_keys[key].append(commodity)
                else:
                    seen_keys[key] = [commodity]

            collisions = {k: v for k, v in seen_keys.items() if len(v) > 1}
            if collisions:
                self.logger.warning(
                    "IOCoefficient commodity_code collision detected: same unique key "
                    "has multiple commodity_codes. Consider adding commodity_code to "
                    "the UniqueConstraint on io_coefficients.",
                    collision_count=len(collisions),
                    sample_key=str(list(collisions.keys())[0]),
                    sample_commodities=collisions[list(collisions.keys())[0]],
                )

            self.logger.info(
                "Stored I-O coefficients",
                count=len(records),
            )

        except Exception as e:
            self.logger.error(
                "Failed to store I-O coefficients",
                error=str(e),
            )
            raise

    async def validate_table_ids(self) -> dict[str, bool]:
        """Validate configured table IDs against the BEA API.

        Checks if the table IDs in IO_TABLES are valid by comparing
        against the GetParameterValues response.

        Returns:
            Dictionary mapping table keys to validity (True if valid)
        """
        if not self.api_key:
            self.logger.warning("BEA API key not configured")
            return {}

        async with httpx.AsyncClient(timeout=30.0) as client:
            available_tables = await self._get_available_tables(client)

        results = {}
        for key, config in IO_TABLES.items():
            is_valid = config.table_id in available_tables
            results[key] = is_valid
            if not is_valid:
                self.logger.warning(
                    "Table ID not found in BEA API",
                    table_key=key,
                    table_id=config.table_id,
                    available_ids=list(available_tables.keys()),
                )

        return results

    async def discover_supply_use_tables(
        self, client: httpx.AsyncClient
    ) -> dict[str, int]:
        """Discover available Supply-Use table IDs from API.

        BEA may change table IDs between releases, so this method
        dynamically discovers the correct IDs for Make, Use, Supply,
        and Import Matrix tables.

        Args:
            client: HTTP client

        Returns:
            Dictionary mapping table type to table ID
        """
        available = await self._get_available_tables(client)
        discovered = {}

        patterns = {
            "make": ["make table", "make, after redefinitions"],
            "use": ["use table", "use, after redefinitions"],
            "supply": ["supply table", "supply, before redefinitions"],
            "import_matrix": ["import matrix", "imports"],
        }

        for table_type, search_patterns in patterns.items():
            for table_id, description in available.items():
                desc_lower = description.lower()
                if any(pattern in desc_lower for pattern in search_patterns):
                    if table_type not in discovered:
                        discovered[table_type] = table_id
                        self.logger.debug(
                            "Discovered table",
                            type=table_type,
                            id=table_id,
                            description=description,
                        )

        return discovered

    async def get_latest_year(self) -> int | None:
        """Get the latest available year of I-O data.

        Returns:
            Latest year or None if unavailable
        """
        if not self.api_key:
            return None

        async with httpx.AsyncClient(timeout=30.0) as client:
            years = await self._get_available_years(client)
            return years[0] if years else None

    async def get_coefficient(
        self,
        from_industry: str,
        to_industry: str,
        year: int | None = None,
        table_type: str = "total_requirements",
        detail_level: str = "summary",
    ) -> Decimal | None:
        """Get a specific I-O coefficient from the database.

        Args:
            from_industry: Source industry code
            to_industry: Consuming industry code
            year: Year (uses latest if None)
            table_type: 'direct_requirements' or 'total_requirements'
            detail_level: 'sector' or 'summary'

        Returns:
            Coefficient value or None
        """
        from src.storage.timescale import get_db
        from src.storage.models import IOCoefficient as IOCoefficientModel
        from sqlalchemy import select

        db = get_db()

        async with db.session() as session:
            query = select(IOCoefficientModel.coefficient).where(
                IOCoefficientModel.from_industry == from_industry,
                IOCoefficientModel.to_industry == to_industry,
                IOCoefficientModel.table_type == table_type,
                IOCoefficientModel.detail_level == detail_level,
            )

            if year:
                query = query.where(IOCoefficientModel.year == year)
            else:
                query = query.order_by(IOCoefficientModel.year.desc())

            result = await session.execute(query.limit(1))
            row = result.scalar_one_or_none()

            return row

    async def collect_with_fallback(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect I-O table data with file-based fallback.

        Tries the API first. If API fails (rate limit, timeout, error),
        falls back to downloading files from BEA's Interactive Tables.

        Args:
            series_ids: Table configuration keys to collect (or all if None)
            start_date: Start year (uses year only)
            end_date: End year (uses year only)

        Returns:
            List of summary DataPoints
        """
        failed_tables: list[str] = []
        table_keys = series_ids or self.get_default_series()

        try:
            results = await self.collect(
                series_ids=table_keys,
                start_date=start_date,
                end_date=end_date,
            )

            collected_tables = {
                dp.series_id.rsplit("_", 1)[0]
                for dp in results
                if "_" in dp.series_id
            }
            failed_tables = [t for t in table_keys if t not in collected_tables]

            if not failed_tables:
                self.logger.info(
                    "API collection successful for all tables",
                    tables=table_keys,
                )
                return results

            self.logger.warning(
                "Some tables failed via API, falling back to file download",
                failed_tables=failed_tables,
            )

        except Exception as e:
            self.logger.error(
                "API collection failed completely, using file fallback",
                error=str(e),
            )
            failed_tables = table_keys
            results = []

        if failed_tables:
            try:
                from src.data_ingestion.collectors.bea_io_file_collector import (
                    get_bea_io_file_collector,
                )

                file_collector = get_bea_io_file_collector(table_keys=failed_tables)
                file_results = await file_collector.collect()

                if file_results:
                    await file_collector.store_coefficients(file_results)
                    results.extend(file_results)
                    self.logger.info(
                        "File fallback collection successful",
                        tables=failed_tables,
                        records=len(file_results),
                    )

            except Exception as e:
                self.logger.error(
                    "File fallback also failed",
                    error=str(e),
                    tables=failed_tables,
                )

        return results


def get_bea_io_collector() -> BEAIOCollector:
    """Factory function to create a BEAIOCollector instance.

    Returns:
        Configured BEAIOCollector instance
    """
    return BEAIOCollector()
