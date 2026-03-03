"""USA Trade Online collector for official U.S. trade statistics."""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.data_ingestion.base_collector import DataFrequency, DataPoint
from src.data_ingestion.file_collector import (
    DatasetConfig,
    FileBasedCollector,
    FileFormat,
)


class USATradeCollector(FileBasedCollector):
    """Collector for USA Trade Online datasets.

    Downloads and processes official U.S. trade statistics including:
    - Imports and exports by commodity
    - Trade by country/region
    - Trade by port/district

    Data source: https://usatrade.census.gov/

    Note: USA Trade Online provides free access but recommends creating
    a free account for better access. This collector uses the public
    data export endpoints.
    """

    # These are examples - actual data is typically accessed via the web interface
    # and exported as CSV. The URLs below are for programmatic access where available.
    DATASETS = {
        "exports_commodity": {
            "url": "https://api.census.gov/data/timeseries/intltrade/exports/hs",
            "description": "U.S. exports by HS commodity code",
            "format": FileFormat.JSON,
            "params": {
                "get": "CTY_CODE,CTY_NAME,ALL_VAL_MO,ALL_VAL_YR",
                "time": "from+2024",
            },
        },
        "imports_commodity": {
            "url": "https://api.census.gov/data/timeseries/intltrade/imports/hs",
            "description": "U.S. imports by HS commodity code",
            "format": FileFormat.JSON,
            "params": {
                "get": "CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR",
                "time": "from+2024",
            },
        },
        "trade_balance": {
            "url": "https://api.census.gov/data/timeseries/intltrade/exports/statehs",
            "description": "Trade balance by state and commodity",
            "format": FileFormat.JSON,
            "params": {
                "get": "STATE,ALL_VAL_MO",
                "time": "from+2024",
            },
        },
    }

    def __init__(
        self,
        data_dir: Path | None = None,
        datasets: list[str] | None = None,
        census_api_key: str | None = None,
    ):
        """Initialize the USA Trade collector.

        Args:
            data_dir: Base directory for storing downloaded files
            datasets: List of dataset IDs to collect (defaults to all)
            census_api_key: Optional Census API key for higher rate limits
        """
        super().__init__(
            name="USA Trade Online",
            source_id="usa_trade",
            data_dir=data_dir,
            timeout=120.0,
            max_retries=3,
        )
        self._selected_datasets = datasets
        self._census_api_key = census_api_key

    def _get_default_headers(self) -> dict[str, str]:
        """Get headers with optional API key."""
        headers = super()._get_default_headers()
        if self._census_api_key:
            headers["X-API-Key"] = self._census_api_key
        return headers

    def get_datasets(self) -> list[DatasetConfig]:
        """Get the list of datasets to collect."""
        datasets = []
        selected = self._selected_datasets or list(self.DATASETS.keys())

        for dataset_id in selected:
            if dataset_id not in self.DATASETS:
                self.logger.warning(
                    "Unknown dataset ID",
                    dataset_id=dataset_id,
                    available=list(self.DATASETS.keys()),
                )
                continue

            config = self.DATASETS[dataset_id]

            url = config["url"]
            if "params" in config:
                params = config["params"].copy()
                if self._census_api_key:
                    params["key"] = self._census_api_key
                param_str = "&".join(f"{k}={v}" for k, v in params.items())
                url = f"{url}?{param_str}"

            datasets.append(
                DatasetConfig(
                    dataset_id=dataset_id,
                    url=url,
                    format=config["format"],
                    filename=f"{dataset_id}.json",
                    description=config["description"],
                    expected_frequency=DataFrequency.MONTHLY,
                    parser_options={},
                )
            )

        return datasets

    def parse_file(
        self,
        downloaded,
        **parser_options,
    ) -> pd.DataFrame:
        """Parse Census API JSON response into DataFrame.

        Census API returns JSON in a specific format:
        [[header1, header2, ...], [val1, val2, ...], ...]
        """
        import json

        content = json.loads(downloaded.content)

        if not content or len(content) < 2:
            return pd.DataFrame()

        # First row is headers, rest are data
        headers = content[0]
        data = content[1:]

        return pd.DataFrame(data, columns=headers)

    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        """Convert a trade dataset DataFrame to DataPoints.

        Args:
            df: Parsed pandas DataFrame
            dataset_id: The dataset identifier

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        if df.empty:
            self.logger.warning("Empty DataFrame for dataset", dataset_id=dataset_id)
            return data_points

        df.columns = df.columns.str.strip().str.lower()

        if dataset_id == "exports_commodity":
            data_points = self._parse_exports(df)
        elif dataset_id == "imports_commodity":
            data_points = self._parse_imports(df)
        elif dataset_id == "trade_balance":
            data_points = self._parse_trade_balance(df)
        else:
            self.logger.warning(
                "No parser defined for dataset",
                dataset_id=dataset_id,
            )

        return data_points

    def _parse_time_column(self, time_val: str) -> datetime | None:
        """Parse Census time format (YYYY-MM or YYYY) to datetime."""
        try:
            if "-" in str(time_val):
                ts = pd.to_datetime(time_val + "-01")
            else:
                ts = pd.to_datetime(f"{time_val}-01-01")
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            return ts.to_pydatetime()
        except (ValueError, TypeError):
            return None

    def _parse_exports(self, df: pd.DataFrame) -> list[DataPoint]:
        """Parse exports dataset."""
        data_points: list[DataPoint] = []

        time_col = next(
            (c for c in ["time", "year", "month"] if c in df.columns), None
        )
        country_col = next(
            (c for c in ["cty_name", "cty_code", "country"] if c in df.columns), None
        )
        value_cols = [
            c for c in df.columns if "val" in c.lower() or "qty" in c.lower()
        ]

        for _, row in df.iterrows():
            try:
                if time_col:
                    timestamp = self._parse_time_column(str(row[time_col]))
                    if not timestamp:
                        continue
                else:
                    timestamp = datetime.now(UTC)

                country = row.get(country_col, "total") if country_col else "total"
                country = str(country).strip().lower().replace(" ", "_")[:50]

                for value_col in value_cols:
                    value = row.get(value_col)
                    if pd.notna(value):
                        try:
                            numeric_value = float(value)
                        except (ValueError, TypeError):
                            continue

                        unit = "usd" if "val" in value_col.lower() else "units"

                        data_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=f"trade.exports.{value_col}.{country}",
                                timestamp=timestamp,
                                value=numeric_value,
                                unit=unit,
                                quality_score=self.calculate_quality_score(),
                                metadata={
                                    "country": country,
                                    "metric": value_col,
                                    "dataset": "exports_commodity",
                                    "flow": "export",
                                },
                            )
                        )
            except Exception as e:
                self.logger.debug("Error parsing export row", error=str(e))
                continue

        return data_points

    def _parse_imports(self, df: pd.DataFrame) -> list[DataPoint]:
        """Parse imports dataset."""
        data_points: list[DataPoint] = []

        time_col = next(
            (c for c in ["time", "year", "month"] if c in df.columns), None
        )
        country_col = next(
            (c for c in ["cty_name", "cty_code", "country"] if c in df.columns), None
        )
        value_cols = [
            c for c in df.columns if "val" in c.lower() or "qty" in c.lower()
        ]

        for _, row in df.iterrows():
            try:
                if time_col:
                    timestamp = self._parse_time_column(str(row[time_col]))
                    if not timestamp:
                        continue
                else:
                    timestamp = datetime.now(UTC)

                country = row.get(country_col, "total") if country_col else "total"
                country = str(country).strip().lower().replace(" ", "_")[:50]

                for value_col in value_cols:
                    value = row.get(value_col)
                    if pd.notna(value):
                        try:
                            numeric_value = float(value)
                        except (ValueError, TypeError):
                            continue

                        unit = "usd" if "val" in value_col.lower() else "units"

                        data_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=f"trade.imports.{value_col}.{country}",
                                timestamp=timestamp,
                                value=numeric_value,
                                unit=unit,
                                quality_score=self.calculate_quality_score(),
                                metadata={
                                    "country": country,
                                    "metric": value_col,
                                    "dataset": "imports_commodity",
                                    "flow": "import",
                                },
                            )
                        )
            except Exception as e:
                self.logger.debug("Error parsing import row", error=str(e))
                continue

        return data_points

    def _parse_trade_balance(self, df: pd.DataFrame) -> list[DataPoint]:
        """Parse trade balance dataset."""
        data_points: list[DataPoint] = []

        time_col = next(
            (c for c in ["time", "year", "month"] if c in df.columns), None
        )
        state_col = next(
            (c for c in ["state", "state_code"] if c in df.columns), None
        )
        value_cols = [c for c in df.columns if "val" in c.lower()]

        for _, row in df.iterrows():
            try:
                if time_col:
                    timestamp = self._parse_time_column(str(row[time_col]))
                    if not timestamp:
                        continue
                else:
                    timestamp = datetime.now(UTC)

                state = row.get(state_col, "total") if state_col else "total"
                state = str(state).strip().lower().replace(" ", "_")[:20]

                for value_col in value_cols:
                    value = row.get(value_col)
                    if pd.notna(value):
                        try:
                            numeric_value = float(value)
                        except (ValueError, TypeError):
                            continue

                        data_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=f"trade.balance.{value_col}.{state}",
                                timestamp=timestamp,
                                value=numeric_value,
                                unit="usd",
                                quality_score=self.calculate_quality_score(),
                                metadata={
                                    "state": state,
                                    "metric": value_col,
                                    "dataset": "trade_balance",
                                },
                            )
                        )
            except Exception as e:
                self.logger.debug("Error parsing trade balance row", error=str(e))
                continue

        return data_points

    def get_schedule(self) -> str:
        """Get the cron schedule for this collector.

        Trade data updates monthly, so check weekly on Tuesdays.
        """
        return "0 7 * * 2"  # Every Tuesday at 7 AM

    @property
    def frequency(self) -> DataFrequency:
        """Get the typical update frequency for trade data."""
        return DataFrequency.MONTHLY
