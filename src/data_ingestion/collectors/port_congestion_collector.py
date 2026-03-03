"""Port congestion data collector with multi-source aggregation.

Collects port congestion data from up to 4 sources in priority order:
1. Global Port Congestion Analytics (free, always runs)
2. Portcast free tier (10 ports, 3 weeks history)
3. Beacon Container Port Congestion Index (API key or scrape)
4. GoComet (3 free searches/week, credit-managed, runs LAST)

Downloaded files use the FileBasedCollector infrastructure for automatic
storage, manifest tracking, and ArchiveManager lifecycle management.
"""

import json
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

KEY_US_PORTS: list[dict[str, str]] = [
    {"code": "USLAX", "name": "Los Angeles"},
    {"code": "USLGB", "name": "Long Beach"},
    {"code": "USNYC", "name": "New York/Newark"},
    {"code": "USSAV", "name": "Savannah"},
    {"code": "USHOU", "name": "Houston"},
    {"code": "USCHI", "name": "Chicago"},
    {"code": "USSEA", "name": "Seattle"},
    {"code": "USORF", "name": "Norfolk"},
    {"code": "USMOB", "name": "Mobile"},
    {"code": "USCHA", "name": "Charleston"},
]

GOCOMET_REDIS_KEY = "gocomet_credits"


class PortCongestionCollector(BaseCollector):
    """Multi-source port congestion data collector.

    Sources are queried in priority order. Each source that succeeds
    adds its data points to the result. Sources that fail are logged
    and skipped gracefully.
    """

    GLOBAL_CONGESTION_URL = (
        "https://globalportcongestion.github.io/blog/data/congestion_data.xlsx"
    )
    PORTCAST_SNAPSHOT_URL = (
        "https://www.portcast.io/port-congestion-sample-dashboard"
    )

    def __init__(self, data_dir: Path | None = None):
        super().__init__(name="Port Congestion", source_id="port_congestion")
        self.settings = get_settings()
        self.rate_limiter = get_rate_limiter("port_congestion")
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw" / "port_congestion"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self._redis = None

    @property
    def frequency(self) -> DataFrequency:
        return DataFrequency.WEEKLY

    def get_schedule(self) -> str:
        return "0 5 * * 2"

    def get_default_series(self) -> list[str]:
        series = [
            "PORT_CONGESTION_ACR_GLOBAL",
            "PORT_CONGESTION_ACT_GLOBAL",
            "PORT_CONGESTION_US_COMPOSITE",
        ]
        for port in KEY_US_PORTS:
            series.append(f"PORT_CONGESTION_{port['code']}")
        return series

    async def validate_api_key(self) -> bool:
        return True

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                points = await self._collect_global_analytics(client)
                data_points.extend(points)
                self.logger.info(
                    "Global Port Congestion Analytics collected",
                    points=len(points),
                )
            except Exception as e:
                self.logger.error(
                    "Global Port Congestion Analytics failed",
                    error=str(e),
                )

            try:
                points = await self._collect_portcast(client)
                data_points.extend(points)
                self.logger.info("Portcast collected", points=len(points))
            except Exception as e:
                self.logger.debug("Portcast collection skipped", error=str(e))

            if self.settings.beacon_api_key:
                try:
                    points = await self._collect_beacon(client)
                    data_points.extend(points)
                    self.logger.info("Beacon collected", points=len(points))
                except Exception as e:
                    self.logger.debug("Beacon collection skipped", error=str(e))

            try:
                points = await self._collect_gocomet(client)
                data_points.extend(points)
            except Exception as e:
                self.logger.debug("GoComet collection skipped", error=str(e))

        composite = self._compute_us_composite(data_points)
        if composite:
            data_points.append(composite)

        return data_points

    async def _collect_global_analytics(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        response = await client.get(self.GLOBAL_CONGESTION_URL, follow_redirects=True)
        if response.status_code == 404:
            self.logger.warning(
                "Global congestion data source returned 404, skipping",
                url=self.GLOBAL_CONGESTION_URL,
            )
            return []
        response.raise_for_status()

        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        save_dir = self.raw_dir / date_str
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / "global_congestion.xlsx"
        file_path.write_bytes(response.content)

        try:
            df = pd.read_excel(file_path, engine="openpyxl")
        except Exception as e:
            self.logger.error("Failed to parse congestion Excel", error=str(e))
            return []

        data_points: list[DataPoint] = []
        now = datetime.now(UTC)

        acr_col = None
        act_col = None
        port_col = None

        for col in df.columns:
            col_lower = str(col).lower()
            if "congestion" in col_lower and "rate" in col_lower:
                acr_col = col
            elif "congestion" in col_lower and "time" in col_lower:
                act_col = col
            elif "port" in col_lower or "name" in col_lower:
                port_col = col

        if acr_col and df[acr_col].notna().any():
            acr_mean = float(df[acr_col].dropna().mean())
            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id="PORT_CONGESTION_ACR_GLOBAL",
                    timestamp=now,
                    value=acr_mean,
                    unit="ratio",
                    metadata={"source": "global_port_congestion_analytics"},
                )
            )

        if act_col and df[act_col].notna().any():
            act_mean = float(df[act_col].dropna().mean())
            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id="PORT_CONGESTION_ACT_GLOBAL",
                    timestamp=now,
                    value=act_mean,
                    unit="days",
                    metadata={"source": "global_port_congestion_analytics"},
                )
            )

        if port_col and act_col:
            for _, row in df.iterrows():
                port_name = str(row.get(port_col, "")).strip()
                act_val = row.get(act_col)
                if pd.isna(act_val) or not port_name:
                    continue

                port_code = self._match_port_code(port_name)
                if port_code:
                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=f"PORT_CONGESTION_{port_code}",
                            timestamp=now,
                            value=float(act_val),
                            unit="days",
                            metadata={
                                "source": "global_port_congestion_analytics",
                                "port_name": port_name,
                            },
                        )
                    )

        return data_points

    async def _collect_portcast(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        if self.settings.portcast_api_key:
            return await self._collect_portcast_api(client)
        return await self._collect_portcast_public(client)

    async def _collect_portcast_api(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        self.logger.info("Portcast API collection not yet configured")
        return []

    async def _collect_portcast_public(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Scrape the public Portcast snapshot page for congestion data."""
        headers = {"User-Agent": self.settings.scraper_user_agent}
        response = await client.get(self.PORTCAST_SNAPSHOT_URL, headers=headers)

        if response.status_code != 200:
            self.logger.debug(
                "Portcast public page unavailable",
                status=response.status_code,
            )
            return []

        data_points: list[DataPoint] = []
        now = datetime.now(UTC)

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, "lxml")

        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue

                port_text = cells[0].get_text(strip=True)
                port_code = self._match_port_code(port_text)
                if not port_code:
                    continue

                try:
                    wait_time = float(
                        cells[1].get_text(strip=True).replace(",", "")
                    )
                except (ValueError, IndexError):
                    continue

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"PORT_CONGESTION_PORTCAST_{port_code}",
                        timestamp=now,
                        value=wait_time,
                        unit="days",
                        metadata={
                            "source": "portcast_public",
                            "port_name": port_text,
                        },
                    )
                )

        return data_points

    async def _collect_beacon(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        self.logger.info("Beacon collection requires API key configuration")
        return []

    async def _collect_gocomet(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Query GoComet with credit management.

        Credit logic:
        - 3 credits/week with weekly refresh
        - If 3 credits (fresh): auto-query silently
        - If 1-2 credits: emit WebSocket event for user prompt (handled
          by the /api/collectors/gocomet/confirm-query endpoint)
        - If 0 credits: skip silently
        """
        credit_state = await self._load_gocomet_credits()
        remaining = credit_state.get("remaining_credits", 3)

        credit_state = self._maybe_refresh_credits(credit_state)
        remaining = credit_state["remaining_credits"]

        if remaining == 0:
            self.logger.debug("GoComet: no credits remaining, skipping")
            return []

        if remaining < 3:
            self.logger.info(
                "GoComet: partial credits, deferring to user prompt",
                remaining=remaining,
            )
            await self._emit_gocomet_prompt(credit_state)
            return []

        data_points = await self._execute_gocomet_query(client)

        credit_state["remaining_credits"] = remaining - 1
        credit_state["last_query_timestamp"] = datetime.now(UTC).isoformat()
        credit_state["queries_this_week"].append(datetime.now(UTC).isoformat())
        await self._save_gocomet_credits(credit_state)

        return data_points

    async def _execute_gocomet_query(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Execute a GoComet port congestion query."""
        headers = {"User-Agent": self.settings.scraper_user_agent}
        data_points: list[DataPoint] = []
        now = datetime.now(UTC)

        for port in KEY_US_PORTS[:3]:
            url = (
                f"https://www.gocomet.com/real-time-port-congestion"
                f"?port={port['name'].lower().replace(' ', '-')}"
            )
            try:
                response = await client.get(url, headers=headers)
                if response.status_code != 200:
                    continue

                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, "lxml")

                delay_el = soup.find(
                    string=lambda t: t and "day" in t.lower() if t else False
                )
                if delay_el:
                    import re
                    match = re.search(r"(\d+(?:\.\d+)?)", str(delay_el))
                    if match:
                        delay_days = float(match.group(1))
                        data_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=f"PORT_CONGESTION_GOCOMET_{port['code']}",
                                timestamp=now,
                                value=delay_days,
                                unit="days",
                                metadata={
                                    "source": "gocomet",
                                    "port_name": port["name"],
                                    "port_code": port["code"],
                                },
                            )
                        )
            except Exception as e:
                self.logger.debug(
                    "GoComet port query failed",
                    port=port["name"],
                    error=str(e),
                )

        self.logger.info("GoComet query complete", points=len(data_points))
        return data_points

    async def _load_gocomet_credits(self) -> dict[str, Any]:
        try:
            from src.storage.redis_cache import get_cache
            cache = get_cache()
            if cache and cache.client:
                raw = await cache.client.get(GOCOMET_REDIS_KEY)
                if raw:
                    return json.loads(raw)
        except Exception:
            pass

        return {
            "remaining_credits": 3,
            "last_query_timestamp": None,
            "credit_refresh_day": "tuesday",
            "refresh_timestamp": None,
            "queries_this_week": [],
        }

    async def _save_gocomet_credits(self, state: dict[str, Any]) -> None:
        try:
            from src.storage.redis_cache import get_cache
            cache = get_cache()
            if cache and cache.client:
                await cache.client.set(
                    GOCOMET_REDIS_KEY,
                    json.dumps(state),
                    ex=604800,
                )
        except Exception as e:
            self.logger.warning("Failed to save GoComet credits", error=str(e))

    def _maybe_refresh_credits(self, state: dict[str, Any]) -> dict[str, Any]:
        """Check if credits should be refreshed based on the refresh day."""
        now = datetime.now(UTC)
        refresh_day = state.get("credit_refresh_day", "tuesday").lower()

        day_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
            "friday": 4, "saturday": 5, "sunday": 6,
        }
        target_weekday = day_map.get(refresh_day, 1)

        if now.weekday() == target_weekday:
            last_query = state.get("last_query_timestamp")
            if last_query:
                last_dt = datetime.fromisoformat(last_query)
                if (now - last_dt).days >= 6:
                    state["remaining_credits"] = 3
                    state["queries_this_week"] = []
                    self.logger.info("GoComet credits refreshed")
            else:
                state["remaining_credits"] = 3
                state["queries_this_week"] = []

        return state

    async def _emit_gocomet_prompt(self, credit_state: dict[str, Any]) -> None:
        """Emit a WebSocket event to prompt the user about GoComet credit usage."""
        try:
            from src.storage.redis_cache import get_cache
            cache = await get_cache()
            if cache:
                remaining = credit_state["remaining_credits"]
                refresh_day = credit_state.get("credit_refresh_day", "tuesday")
                now = datetime.now(UTC)

                day_map = {
                    "monday": 0, "tuesday": 1, "wednesday": 2,
                    "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
                }
                target = day_map.get(refresh_day, 1)
                days_until = (target - now.weekday()) % 7
                if days_until == 0:
                    days_until = 7
                hours_until = days_until * 24

                event = {
                    "type": "gocomet_credit_prompt",
                    "remaining_credits": remaining,
                    "hours_until_refresh": hours_until,
                    "refresh_day": refresh_day,
                }
                await cache.client.publish("notifications", json.dumps(event))
                self.logger.info(
                    "GoComet credit prompt emitted",
                    remaining=remaining,
                    hours_until_refresh=hours_until,
                )
        except Exception as e:
            self.logger.warning("Failed to emit GoComet prompt", error=str(e))

    def _match_port_code(self, port_name: str) -> str | None:
        """Match a port name string to a known US port code."""
        name_lower = port_name.lower()
        mapping = {
            "los angeles": "USLAX",
            "la": "USLAX",
            "long beach": "USLGB",
            "new york": "USNYC",
            "newark": "USNYC",
            "ny/nj": "USNYC",
            "savannah": "USSAV",
            "houston": "USHOU",
            "chicago": "USCHI",
            "seattle": "USSEA",
            "norfolk": "USORF",
            "mobile": "USMOB",
            "charleston": "USCHA",
        }
        for pattern, code in mapping.items():
            if pattern in name_lower:
                return code
        return None

    def _compute_us_composite(
        self,
        data_points: list[DataPoint],
    ) -> DataPoint | None:
        """Compute a weighted composite of US port congestion."""
        us_port_codes = {p["code"] for p in KEY_US_PORTS}
        port_values: dict[str, float] = {}

        for dp in data_points:
            for code in us_port_codes:
                if dp.series_id.endswith(code) and dp.unit == "days":
                    if code not in port_values or dp.value > port_values[code]:
                        port_values[code] = dp.value

        if not port_values:
            return None

        weights = {
            "USLAX": 0.20, "USLGB": 0.18, "USNYC": 0.15,
            "USSAV": 0.12, "USHOU": 0.10, "USSEA": 0.07,
            "USORF": 0.06, "USCHA": 0.05, "USMOB": 0.04,
            "USCHI": 0.03,
        }

        weighted_sum = 0.0
        weight_total = 0.0
        for code, value in port_values.items():
            w = weights.get(code, 0.05)
            weighted_sum += value * w
            weight_total += w

        if weight_total == 0:
            return None

        composite = weighted_sum / weight_total

        return DataPoint(
            source_id=self.source_id,
            series_id="PORT_CONGESTION_US_COMPOSITE",
            timestamp=datetime.now(UTC),
            value=composite,
            unit="days",
            metadata={
                "source": "composite",
                "ports_included": list(port_values.keys()),
                "port_count": len(port_values),
            },
        )
