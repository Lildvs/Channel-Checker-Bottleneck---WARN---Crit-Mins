"""VA Open Data Portal collector for veterans healthcare data."""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.data_ingestion.base_collector import DataFrequency, DataPoint
from src.data_ingestion.file_collector import (
    DatasetConfig,
    FileBasedCollector,
    FileFormat,
)


class VAHealthcareCollector(FileBasedCollector):
    """Collector for VA Open Data Portal healthcare datasets.

    Downloads and processes veterans healthcare utilization data including:
    - Wait times by facility
    - Patient satisfaction scores
    - Healthcare access metrics

    Data source: https://www.data.va.gov/
    """

    DATASETS = {
        "healthcare_utilization": {
            "url": "https://www.data.va.gov/resource/syfx-f9ne.csv?$limit=50000",
            "description": "VA healthcare utilization metrics by state (FY2022-2024)",
            "format": FileFormat.CSV,
        },
        "facility_counts": {
            "url": "https://www.data.va.gov/resource/2weg-acgq.csv?$limit=50000",
            "description": "VA facilities aggregated counts by state (FY2025)",
            "format": FileFormat.CSV,
        },
        "benefits_utilization": {
            "url": "https://www.data.va.gov/resource/8x3h-hffb.csv?$limit=50000",
            "description": "VA benefits utilization by state (FY2022-2024)",
            "format": FileFormat.CSV,
        },
    }

    def __init__(
        self,
        data_dir: Path | None = None,
        datasets: list[str] | None = None,
    ):
        """Initialize the VA Healthcare collector.

        Args:
            data_dir: Base directory for storing downloaded files
            datasets: List of dataset IDs to collect (defaults to all)
        """
        super().__init__(
            name="VA Healthcare",
            source_id="va_healthcare",
            data_dir=data_dir,
            timeout=120.0,  # VA downloads can be slow
            max_retries=3,
        )
        self._selected_datasets = datasets

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
            datasets.append(
                DatasetConfig(
                    dataset_id=dataset_id,
                    url=config["url"],
                    format=config["format"],
                    filename=f"{dataset_id}.csv",
                    description=config["description"],
                    expected_frequency=DataFrequency.QUARTERLY,
                    parser_options={
                        "low_memory": False,
                        "na_values": ["", "NA", "N/A", "null", "NULL"],
                    },
                )
            )

        return datasets

    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        """Convert a VA dataset DataFrame to DataPoints.

        The VA Socrata datasets use a wide format with columns named
        ``{state}_{year}`` (e.g. ``alabama_2024``).  This parser melts
        them into individual observations.

        Args:
            df: Parsed pandas DataFrame
            dataset_id: The dataset identifier

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        if dataset_id in ("healthcare_utilization", "benefits_utilization"):
            data_points = self._parse_wide_state_year(df, dataset_id)
        elif dataset_id == "facility_counts":
            data_points = self._parse_facility_counts(df)
        else:
            self.logger.warning("No parser for dataset", dataset_id=dataset_id)

        return data_points

    def _parse_wide_state_year(
        self, df: pd.DataFrame, dataset_id: str,
    ) -> list[DataPoint]:
        """Parse wide-format VA data with ``{state}_{year}`` columns."""
        import re as _re

        data_points: list[DataPoint] = []
        now = datetime.now(UTC)

        cat_col = next((c for c in df.columns if c in ("cat", "category")), None)
        if cat_col is None:
            self.logger.warning("No category column found", dataset_id=dataset_id)
            return data_points

        state_year_pattern = _re.compile(r"^(.+)_(\d{4})$")

        for _, row in df.iterrows():
            category = str(row.get(cat_col, "")).strip()
            if not category:
                continue

            for col in df.columns:
                if col == cat_col:
                    continue
                m = state_year_pattern.match(col)
                if not m:
                    continue

                state, year_str = m.group(1), m.group(2)
                value = row.get(col)
                if pd.isna(value):
                    continue
                try:
                    numeric_value = float(value)
                except (ValueError, TypeError):
                    continue

                timestamp = datetime(int(year_str), 1, 1, tzinfo=UTC)
                series_id = (
                    f"va.{dataset_id}.{category.replace(' ', '_')[:40]}.{state}"
                )

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=series_id,
                        timestamp=timestamp,
                        value=numeric_value,
                        unit="count",
                        quality_score=self.calculate_quality_score(),
                        metadata={
                            "state": state,
                            "category": category,
                            "dataset": dataset_id,
                            "year": year_str,
                        },
                    )
                )

        return data_points

    def _parse_facility_counts(self, df: pd.DataFrame) -> list[DataPoint]:
        """Parse facility-counts dataset (one row per facility type)."""
        data_points: list[DataPoint] = []
        now = datetime.now(UTC)

        type_col = next(
            (c for c in df.columns if "facility" in c or "type" in c), None
        )
        if type_col is None:
            type_col = df.columns[0]

        for _, row in df.iterrows():
            facility_type = str(row.get(type_col, "unknown")).strip()
            if not facility_type:
                continue

            for col in df.columns:
                if col == type_col:
                    continue
                value = row.get(col)
                if pd.isna(value):
                    continue
                try:
                    numeric_value = float(value)
                except (ValueError, TypeError):
                    continue

                state = col.replace("_", " ").strip()
                series_id = f"va.facilities.{facility_type.replace(' ', '_')[:40]}.{col}"

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=series_id,
                        timestamp=now,
                        value=numeric_value,
                        unit="count",
                        quality_score=self.calculate_quality_score(),
                        metadata={
                            "facility_type": facility_type,
                            "state": state,
                            "dataset": "facility_counts",
                        },
                    )
                )

        return data_points

    def get_schedule(self) -> str:
        """Get the cron schedule for this collector.

        VA data updates quarterly, so check weekly on Mondays.
        """
        return "0 6 * * 1"  # Every Monday at 6 AM

    @property
    def frequency(self) -> DataFrequency:
        """Get the typical update frequency for VA data."""
        return DataFrequency.QUARTERLY
