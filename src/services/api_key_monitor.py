"""API Key Expiration Monitoring Service.

Tracks API key expiration dates and sends alerts before keys expire.
"""

from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class APIKeyStatus:
    """Status of an API key."""
    
    key_name: str
    env_var_name: str
    is_configured: bool
    expires_at: datetime | None
    days_until_expiry: int | None
    status: str  # "ok", "expiring_soon", "expired", "not_configured", "no_expiration"
    notes: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key_name": self.key_name,
            "env_var_name": self.env_var_name,
            "is_configured": self.is_configured,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "days_until_expiry": self.days_until_expiry,
            "status": self.status,
            "notes": self.notes,
        }


# Known API key information
# Format: "setting_attr": ("Display Name", "expiry_date or None", "notes")
API_KEY_REGISTRY: dict[str, tuple[str, str | None, str | None]] = {
    # Government APIs (generally no expiration, but may have annual renewal)
    "fred_api_key": ("FRED API Key", None, "Federal Reserve - no expiration"),
    "bls_api_key": ("BLS API Key", None, "Bureau of Labor Statistics - no expiration"),
    "bea_api_key": ("BEA API Key", None, "Bureau of Economic Analysis - no expiration"),
    "eia_api_key": ("EIA API Key", None, "Energy Information Administration - no expiration"),
    "census_api_key": ("Census API Key", None, "US Census Bureau - no expiration"),
    "usda_nass_api_key": ("USDA NASS API Key", None, "USDA NASS - no expiration"),
    "noaa_api_key": ("NOAA API Key", None, "NOAA - no expiration"),
    
    # International/Commercial APIs (may have expiration)
    "un_comtrade_api_key": ("UN Comtrade API Key", None, "Check annual renewal requirements"),
    "opensecrets_api_key": ("OpenSecrets API Key", None, "Check renewal requirements"),
    
    # LLM APIs (typically no expiration, but billing-dependent)
    "openai_api_key": ("OpenAI API Key", None, "No expiration - billing dependent"),
    "anthropic_api_key": ("Anthropic API Key", None, "No expiration - billing dependent"),
    "openrouter_api_key": ("OpenRouter API Key", None, "No expiration - billing dependent"),
}


class APIKeyMonitor:
    """Service for monitoring API key status and expiration.
    
    Checks which keys are configured and tracks expiration dates.
    """
    
    # Days before expiry to trigger warnings
    WARNING_THRESHOLD_DAYS = 30
    CRITICAL_THRESHOLD_DAYS = 7
    
    def __init__(self, settings=None, notification_service=None):
        """Initialize the API key monitor.
        
        Args:
            settings: Application settings instance
            notification_service: NotificationService for alerts
        """
        self._settings = settings
        self._notification_service = notification_service
        self.logger = logger.bind(component="APIKeyMonitor")
        
        # Custom expiration overrides (can be set at runtime)
        self._custom_expirations: dict[str, datetime] = {}
    
    @property
    def settings(self):
        """Get settings instance."""
        if self._settings is None:
            from src.config.settings import get_settings
            self._settings = get_settings()
        return self._settings
    
    @property
    def notification_service(self):
        """Get notification service instance."""
        if self._notification_service is None:
            from src.services.notification_service import NotificationService, NotificationConfig
            config = NotificationConfig.from_settings(self.settings)
            self._notification_service = NotificationService(config)
        return self._notification_service
    
    def set_expiration(self, key_attr: str, expires_at: datetime) -> None:
        """Set a custom expiration date for an API key.
        
        Args:
            key_attr: Settings attribute name (e.g., "un_comtrade_api_key")
            expires_at: Expiration datetime
        """
        self._custom_expirations[key_attr] = expires_at
        self.logger.info(
            "API key expiration set",
            key=key_attr,
            expires_at=expires_at.isoformat(),
        )
    
    def _is_key_configured(self, key_attr: str) -> bool:
        """Check if an API key is configured (non-empty).
        
        Args:
            key_attr: Settings attribute name
            
        Returns:
            True if key is configured
        """
        key_value = getattr(self.settings, key_attr, None)
        
        if key_value is None:
            return False
        
        if hasattr(key_value, 'get_secret_value'):
            value = key_value.get_secret_value()
        else:
            value = str(key_value)
        
        return bool(value and value.strip())
    
    def _get_expiration(self, key_attr: str) -> datetime | None:
        """Get expiration date for an API key.
        
        Args:
            key_attr: Settings attribute name
            
        Returns:
            Expiration datetime or None
        """
        if key_attr in self._custom_expirations:
            return self._custom_expirations[key_attr]
        
        if key_attr in API_KEY_REGISTRY:
            _, expiry_str, _ = API_KEY_REGISTRY[key_attr]
            if expiry_str:
                try:
                    return datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=UTC)
                except ValueError:
                    self.logger.warning(f"Invalid expiration date format: {expiry_str}")
        
        return None
    
    def check_key(self, key_attr: str) -> APIKeyStatus:
        """Check the status of a single API key.
        
        Args:
            key_attr: Settings attribute name
            
        Returns:
            APIKeyStatus for the key
        """
        if key_attr in API_KEY_REGISTRY:
            display_name, _, notes = API_KEY_REGISTRY[key_attr]
        else:
            display_name = key_attr.upper()
            notes = None
        
        is_configured = self._is_key_configured(key_attr)
        expires_at = self._get_expiration(key_attr)
        
        days_until_expiry = None
        if expires_at:
            delta = expires_at - datetime.now(UTC)
            days_until_expiry = delta.days
        
        if not is_configured:
            status = "not_configured"
        elif expires_at is None:
            status = "no_expiration"
        elif days_until_expiry is not None and days_until_expiry < 0:
            status = "expired"
        elif days_until_expiry is not None and days_until_expiry <= self.CRITICAL_THRESHOLD_DAYS:
            status = "expiring_soon"
        elif days_until_expiry is not None and days_until_expiry <= self.WARNING_THRESHOLD_DAYS:
            status = "expiring_soon"
        else:
            status = "ok"
        
        return APIKeyStatus(
            key_name=display_name,
            env_var_name=key_attr.upper(),
            is_configured=is_configured,
            expires_at=expires_at,
            days_until_expiry=days_until_expiry,
            status=status,
            notes=notes,
        )
    
    def check_all_keys(self) -> list[APIKeyStatus]:
        """Check status of all registered API keys.
        
        Returns:
            List of APIKeyStatus for all keys
        """
        statuses = []
        
        for key_attr in API_KEY_REGISTRY:
            status = self.check_key(key_attr)
            statuses.append(status)
        
        return statuses
    
    def get_configured_keys(self) -> list[APIKeyStatus]:
        """Get status of all configured keys only.
        
        Returns:
            List of APIKeyStatus for configured keys
        """
        return [s for s in self.check_all_keys() if s.is_configured]
    
    def get_keys_needing_attention(self) -> list[APIKeyStatus]:
        """Get keys that need attention (expired or expiring soon).
        
        Returns:
            List of APIKeyStatus for problematic keys
        """
        return [
            s for s in self.check_all_keys()
            if s.status in ("expired", "expiring_soon")
        ]
    
    def get_summary(self) -> dict[str, Any]:
        """Get a summary of all API key statuses.
        
        Returns:
            Summary dictionary with counts and details
        """
        all_statuses = self.check_all_keys()
        
        return {
            "total_keys": len(all_statuses),
            "configured": sum(1 for s in all_statuses if s.is_configured),
            "not_configured": sum(1 for s in all_statuses if not s.is_configured),
            "ok": sum(1 for s in all_statuses if s.status == "ok"),
            "no_expiration": sum(1 for s in all_statuses if s.status == "no_expiration"),
            "expiring_soon": sum(1 for s in all_statuses if s.status == "expiring_soon"),
            "expired": sum(1 for s in all_statuses if s.status == "expired"),
            "keys": [s.to_dict() for s in all_statuses],
        }
    
    async def send_expiration_alerts(self) -> int:
        """Send alerts for expiring/expired keys.
        
        Returns:
            Number of alerts sent
        """
        alerts_sent = 0
        
        for status in self.get_keys_needing_attention():
            if status.days_until_expiry is not None:
                await self.notification_service.notify_api_key_expiring(
                    key_name=status.key_name,
                    days_until_expiry=status.days_until_expiry,
                )
                alerts_sent += 1
                
                self.logger.warning(
                    "API key expiration alert sent",
                    key=status.key_name,
                    days_until_expiry=status.days_until_expiry,
                )
        
        return alerts_sent


# Global instance
_monitor: APIKeyMonitor | None = None


def get_api_key_monitor() -> APIKeyMonitor:
    """Get the global API key monitor instance.
    
    Returns:
        APIKeyMonitor singleton
    """
    global _monitor
    if _monitor is None:
        _monitor = APIKeyMonitor()
    return _monitor
