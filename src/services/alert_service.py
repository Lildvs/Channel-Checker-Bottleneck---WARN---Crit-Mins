"""Alert Service for prioritization, deduplication, and history management.

Provides centralized alert management with:
- Priority classification based on severity and category
- Deduplication to prevent alert fatigue
- History storage and retrieval
- Redis integration for real-time operations
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from enum import Enum
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

import structlog

from src.analysis.signals import BottleneckCategory
from src.storage.redis_cache import RedisCache, get_cache

logger = structlog.get_logger()


class AlertPriority(str, Enum):
    """Alert priority levels."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @classmethod
    def from_severity(cls, severity: float, category: BottleneckCategory | None = None) -> "AlertPriority":
        """Determine priority from severity score and category.

        Args:
            severity: Severity score (0-1)
            category: Optional bottleneck category for priority boosting

        Returns:
            AlertPriority enum value
        """
        # Critical categories get priority boost
        critical_categories = {
            BottleneckCategory.ENERGY_CRUNCH,
            BottleneckCategory.SUPPLY_DISRUPTION,
        }

        boost = 0.1 if category in critical_categories else 0.0
        adjusted_severity = min(1.0, severity + boost)

        if adjusted_severity >= 0.8:
            return cls.CRITICAL
        elif adjusted_severity >= 0.6:
            return cls.HIGH
        elif adjusted_severity >= 0.4:
            return cls.MEDIUM
        else:
            return cls.LOW


@dataclass
class AlertRecord:
    """Represents a processed alert with metadata."""

    id: UUID
    alert_type: str  # bottleneck, price_spike, inventory, etc.
    category: str
    severity: float
    priority: AlertPriority
    description: str
    dedup_key: str
    affected_sectors: list[str] = field(default_factory=list)
    source_series: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    acknowledged_at: datetime | None = None
    expires_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": str(self.id),
            "alert_type": self.alert_type,
            "category": self.category,
            "severity": self.severity,
            "priority": self.priority.value,
            "description": self.description,
            "dedup_key": self.dedup_key,
            "affected_sectors": self.affected_sectors,
            "source_series": self.source_series,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AlertRecord":
        """Create AlertRecord from dictionary."""
        return cls(
            id=UUID(data["id"]),
            alert_type=data["alert_type"],
            category=data["category"],
            severity=data["severity"],
            priority=AlertPriority(data["priority"]),
            description=data["description"],
            dedup_key=data["dedup_key"],
            affected_sectors=data.get("affected_sectors", []),
            source_series=data.get("source_series", []),
            payload=data.get("payload", {}),
            created_at=datetime.fromisoformat(data["created_at"]),
            acknowledged_at=(
                datetime.fromisoformat(data["acknowledged_at"])
                if data.get("acknowledged_at")
                else None
            ),
            expires_at=(
                datetime.fromisoformat(data["expires_at"])
                if data.get("expires_at")
                else None
            ),
        )


# Deduplication time windows by priority
DEDUP_WINDOWS = {
    AlertPriority.CRITICAL: timedelta(minutes=5),
    AlertPriority.HIGH: timedelta(minutes=15),
    AlertPriority.MEDIUM: timedelta(minutes=30),
    AlertPriority.LOW: timedelta(hours=1),
}

# Alert expiration times
ALERT_TTL = {
    AlertPriority.CRITICAL: timedelta(hours=24),
    AlertPriority.HIGH: timedelta(hours=12),
    AlertPriority.MEDIUM: timedelta(hours=6),
    AlertPriority.LOW: timedelta(hours=3),
}

# Redis key prefixes
REDIS_ALERT_HISTORY = "alerts:history"
REDIS_ALERT_DEDUP = "alerts:dedup"
REDIS_ALERT_STATS = "alerts:stats"


class AlertService:
    """Service for managing alerts with prioritization and deduplication."""

    def __init__(self, cache: RedisCache | None = None):
        """Initialize the alert service.

        Args:
            cache: Redis cache instance (uses global if not provided)
        """
        self._cache = cache
        self.logger = logger.bind(component="AlertService")

    @property
    def cache(self) -> RedisCache:
        """Get Redis cache instance."""
        if self._cache is None:
            self._cache = get_cache()
        return self._cache

    def generate_dedup_key(
        self,
        category: str,
        affected_sectors: list[str],
        time_bucket_minutes: int = 15,
    ) -> str:
        """Generate a deduplication key for an alert.

        Args:
            category: Alert category
            affected_sectors: List of affected sector codes
            time_bucket_minutes: Time bucket size for grouping

        Returns:
            SHA256 hash as dedup key
        """
        now = datetime.now(UTC)
        bucket = now.replace(
            minute=(now.minute // time_bucket_minutes) * time_bucket_minutes,
            second=0,
            microsecond=0,
        )

        sectors_str = ",".join(sorted(affected_sectors))
        key_str = f"{category}:{sectors_str}:{bucket.isoformat()}"

        return sha256(key_str.encode()).hexdigest()[:16]

    async def is_duplicate(self, dedup_key: str) -> bool:
        """Check if an alert is a duplicate.

        Args:
            dedup_key: Deduplication key

        Returns:
            True if duplicate exists in Redis
        """
        try:
            redis_key = f"{REDIS_ALERT_DEDUP}:{dedup_key}"
            result = await self.cache.get(redis_key)
            return result is not None
        except Exception as e:
            self.logger.warning("Failed to check dedup", error=str(e))
            return False

    async def mark_seen(self, dedup_key: str, ttl_seconds: int = 900) -> None:
        """Mark a dedup key as seen.

        Args:
            dedup_key: Deduplication key
            ttl_seconds: Time to live for the marker
        """
        try:
            redis_key = f"{REDIS_ALERT_DEDUP}:{dedup_key}"
            await self.cache.set(redis_key, "1", ttl=ttl_seconds)
        except Exception as e:
            self.logger.warning("Failed to mark seen", error=str(e))

    def prioritize_alert(
        self,
        severity: float,
        category: str,
        affected_sectors: list[str] | None = None,
    ) -> AlertPriority:
        """Determine alert priority based on severity and context.

        Args:
            severity: Severity score (0-1)
            category: Alert category string
            affected_sectors: Optional list of affected sectors

        Returns:
            AlertPriority enum value
        """
        try:
            bottleneck_cat = BottleneckCategory(category)
        except ValueError:
            bottleneck_cat = None

        priority = AlertPriority.from_severity(severity, bottleneck_cat)

        # Boost priority if many sectors affected
        if affected_sectors and len(affected_sectors) >= 5:
            if priority == AlertPriority.MEDIUM:
                priority = AlertPriority.HIGH
            elif priority == AlertPriority.LOW:
                priority = AlertPriority.MEDIUM

        return priority

    async def create_alert(
        self,
        alert_type: str,
        category: str,
        severity: float,
        description: str,
        affected_sectors: list[str] | None = None,
        source_series: list[str] | None = None,
        payload: dict[str, Any] | None = None,
        skip_dedup: bool = False,
    ) -> AlertRecord | None:
        """Create a new alert with deduplication check.

        Args:
            alert_type: Type of alert
            category: Category string
            severity: Severity score (0-1)
            description: Human-readable description
            affected_sectors: List of affected sector codes
            source_series: List of source data series
            payload: Additional data payload
            skip_dedup: Skip deduplication check

        Returns:
            AlertRecord if created, None if duplicate
        """
        affected_sectors = affected_sectors or []
        source_series = source_series or []
        payload = payload or {}

        dedup_key = self.generate_dedup_key(category, affected_sectors)

        if not skip_dedup and await self.is_duplicate(dedup_key):
            self.logger.debug("Duplicate alert suppressed", dedup_key=dedup_key)
            return None

        priority = self.prioritize_alert(severity, category, affected_sectors)

        alert = AlertRecord(
            id=uuid4(),
            alert_type=alert_type,
            category=category,
            severity=severity,
            priority=priority,
            description=description,
            dedup_key=dedup_key,
            affected_sectors=affected_sectors,
            source_series=source_series,
            payload=payload,
            expires_at=datetime.now(UTC) + ALERT_TTL[priority],
        )

        dedup_window = DEDUP_WINDOWS[priority]
        await self.mark_seen(dedup_key, int(dedup_window.total_seconds()))

        await self.store_alert(alert)

        await self._update_stats(alert)

        self.logger.info(
            "Alert created",
            alert_id=str(alert.id),
            priority=priority.value,
            category=category,
        )

        return alert

    async def store_alert(self, alert: AlertRecord) -> None:
        """Store alert in Redis sorted set for history.

        Args:
            alert: AlertRecord to store
        """
        try:
            import json

            score = alert.created_at.timestamp()
            member = json.dumps(alert.to_dict())

            await self.cache.redis.zadd(REDIS_ALERT_HISTORY, {member: score})

            await self.cache.redis.expire(REDIS_ALERT_HISTORY, 86400 * 7)  # 7 days

        except Exception as e:
            self.logger.error("Failed to store alert", error=str(e))

    async def get_alert_history(
        self,
        limit: int = 50,
        offset: int = 0,
        priority_filter: list[AlertPriority] | None = None,
        category_filter: str | None = None,
        unacknowledged_only: bool = False,
    ) -> list[AlertRecord]:
        """Retrieve alert history with filtering.

        Args:
            limit: Maximum number of alerts
            offset: Offset for pagination
            priority_filter: Filter by priorities
            category_filter: Filter by category
            unacknowledged_only: Only return unacknowledged alerts

        Returns:
            List of AlertRecord objects
        """
        try:
            import json

            start = offset
            end = offset + limit * 3  # Fetch extra for filtering

            members = await self.cache.redis.zrevrange(
                REDIS_ALERT_HISTORY, start, end
            )

            alerts = []
            for member in members:
                try:
                    data = json.loads(member)
                    alert = AlertRecord.from_dict(data)

                    if priority_filter and alert.priority not in priority_filter:
                        continue
                    if category_filter and alert.category != category_filter:
                        continue
                    if unacknowledged_only and alert.acknowledged_at is not None:
                        continue

                    alerts.append(alert)

                    if len(alerts) >= limit:
                        break

                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    self.logger.warning("Failed to parse alert", error=str(e))
                    continue

            return alerts

        except Exception as e:
            self.logger.error("Failed to get alert history", error=str(e))
            return []

    async def get_recent_unacknowledged(
        self,
        limit: int = 20,
        priority_filter: list[AlertPriority] | None = None,
    ) -> list[AlertRecord]:
        """Get recent unacknowledged alerts for SSE initial batch.

        Args:
            limit: Maximum number of alerts
            priority_filter: Filter by priorities

        Returns:
            List of unacknowledged AlertRecord objects
        """
        return await self.get_alert_history(
            limit=limit,
            priority_filter=priority_filter,
            unacknowledged_only=True,
        )

    async def acknowledge_alert(self, alert_id: UUID) -> bool:
        """Mark an alert as acknowledged.

        Args:
            alert_id: UUID of the alert

        Returns:
            True if acknowledged, False if not found
        """
        try:
            import json

            members = await self.cache.redis.zrange(REDIS_ALERT_HISTORY, 0, -1)

            for member in members:
                try:
                    data = json.loads(member)
                    if data["id"] == str(alert_id):
                        data["acknowledged_at"] = datetime.now(UTC).isoformat()

                        await self.cache.redis.zrem(REDIS_ALERT_HISTORY, member)
                        score = datetime.fromisoformat(data["created_at"]).timestamp()
                        await self.cache.redis.zadd(
                            REDIS_ALERT_HISTORY, {json.dumps(data): score}
                        )

                        self.logger.info("Alert acknowledged", alert_id=str(alert_id))
                        return True

                except (json.JSONDecodeError, KeyError):
                    continue

            return False

        except Exception as e:
            self.logger.error("Failed to acknowledge alert", error=str(e))
            return False

    async def _update_stats(self, alert: AlertRecord) -> None:
        """Update alert statistics in Redis.

        Args:
            alert: Alert that was created
        """
        try:
            today = datetime.now(UTC).strftime("%Y-%m-%d")

            await self.cache.redis.hincrby(f"{REDIS_ALERT_STATS}:{today}", "total", 1)
            await self.cache.redis.hincrby(
                f"{REDIS_ALERT_STATS}:{today}", f"priority:{alert.priority.value}", 1
            )
            await self.cache.redis.hincrby(
                f"{REDIS_ALERT_STATS}:{today}", f"category:{alert.category}", 1
            )

            await self.cache.redis.expire(f"{REDIS_ALERT_STATS}:{today}", 86400 * 30)

        except Exception as e:
            self.logger.warning("Failed to update stats", error=str(e))

    async def get_stats(self, days: int = 7) -> dict[str, Any]:
        """Get alert statistics.

        Args:
            days: Number of days to include

        Returns:
            Statistics dictionary
        """
        try:
            stats = {
                "total": 0,
                "by_priority": {},
                "by_category": {},
                "by_day": {},
            }

            for i in range(days):
                date = (datetime.now(UTC) - timedelta(days=i)).strftime("%Y-%m-%d")
                day_stats = await self.cache.redis.hgetall(f"{REDIS_ALERT_STATS}:{date}")

                if day_stats:
                    day_total = int(day_stats.get("total", 0))
                    stats["total"] += day_total
                    stats["by_day"][date] = day_total

                    for key, value in day_stats.items():
                        if key.startswith("priority:"):
                            priority = key.split(":")[1]
                            stats["by_priority"][priority] = (
                                stats["by_priority"].get(priority, 0) + int(value)
                            )
                        elif key.startswith("category:"):
                            category = key.split(":")[1]
                            stats["by_category"][category] = (
                                stats["by_category"].get(category, 0) + int(value)
                            )

            return stats

        except Exception as e:
            self.logger.error("Failed to get stats", error=str(e))
            return {"total": 0, "by_priority": {}, "by_category": {}, "by_day": {}}

    async def publish_alert(self, alert: AlertRecord) -> None:
        """Publish alert to Redis pub/sub for SSE streaming.

        Args:
            alert: Alert to publish
        """
        try:
            await self.cache.publish_bottleneck_alert(alert.to_dict())
        except Exception as e:
            self.logger.error("Failed to publish alert", error=str(e))
