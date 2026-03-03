"""Application settings using Pydantic Settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment
    environment: Literal["development", "staging", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # Database
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "changeme"
    db_user: str = "changeme"
    db_password: SecretStr = SecretStr("changeme")

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: SecretStr | None = None
    redis_db: int = 0

    # API Keys - Critical
    fred_api_key: SecretStr | None = None
    bls_api_key: SecretStr | None = None
    bea_api_key: SecretStr | None = None
    eia_api_key: SecretStr | None = None
    census_api_key: SecretStr | None = None

    # API Keys - Important
    usda_nass_api_key: SecretStr | None = None
    noaa_api_key: SecretStr | None = None
    un_comtrade_api_key: SecretStr | None = None
    opensecrets_api_key: SecretStr | None = None

    # Port Congestion / Supply Disruption APIs
    portcast_api_key: SecretStr | None = None
    beacon_api_key: SecretStr | None = None
    gocomet_api_key: SecretStr | None = None

    # Google Trends (official API alpha -- placeholders for future migration from pytrends)
    google_trends_api_key: SecretStr | None = None
    google_trends_oauth_client: str | None = None

    # SEC EDGAR Configuration (required by SEC policy)
    sec_user_agent: str = Field(
        default="ChannelCheck/1.0 contact@example.com",
        description="User-Agent string for SEC EDGAR API (required per SEC policy)",
    )
    scraper_user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        description="Browser user-agent string for web scraping collectors",
    )
    sec_monitored_ciks: list[str] = Field(
        default_factory=list,
        description="List of company CIKs to monitor (comma-separated in env var)",
    )

    # LLM API Keys (at least one required for LLM-powered forecasting)
    openai_api_key: SecretStr | None = None
    anthropic_api_key: SecretStr | None = None
    openrouter_api_key: SecretStr | None = None

    # Forecasting Settings
    forecasting_use_llm: bool = True
    forecasting_default_model: str = "gpt-5"
    forecasting_max_cost_per_forecast: float = 3.50  # USD

    # Rate Limiting - External APIs (requests per minute)
    fred_rate_limit: int = Field(default=120, description="FRED API rate limit per minute")
    bls_rate_limit: int = Field(default=25, description="BLS API rate limit per minute")
    bea_rate_limit: int = Field(default=100, description="BEA API rate limit per minute")
    eia_rate_limit: int = Field(default=60, description="EIA API rate limit per minute")
    census_rate_limit: int = Field(default=50, description="Census API rate limit per minute")

    # Rate Limiting - API Endpoints
    api_rate_limit_enabled: bool = Field(
        default=True,
        description="Enable rate limiting for API endpoints",
    )
    api_rate_limit: int = Field(
        default=1000,
        description="Maximum API requests per IP per minute",
    )
    api_rate_limit_window: int = Field(
        default=60,
        description="Rate limit time window in seconds",
    )

    # Database Connection Pool
    pool_size: int = Field(default=10, description="Database connection pool size")
    max_overflow: int = Field(default=20, description="Maximum pool overflow connections")

    # Notification Settings
    slack_webhook_url: str | None = Field(
        default=None,
        description="Slack webhook URL for notifications",
    )
    slack_channel: str | None = Field(
        default=None,
        description="Override Slack channel for notifications",
    )
    smtp_host: str | None = Field(default=None, description="SMTP server host")
    smtp_port: int = Field(default=587, description="SMTP server port")
    smtp_use_tls: bool = Field(default=True, description="Use TLS for SMTP")
    smtp_user: str | None = Field(default=None, description="SMTP username")
    smtp_password: SecretStr | None = Field(default=None, description="SMTP password")
    notification_email_from: str | None = Field(
        default=None,
        description="Email sender address for notifications",
    )
    notification_email_to: list[str] = Field(
        default_factory=list,
        description="Email recipients for notifications",
    )

    @property
    def database_url(self) -> str:
        """Construct async database URL."""
        password = self.db_password.get_secret_value()
        return f"postgresql+asyncpg://{self.db_user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def sync_database_url(self) -> str:
        """Construct sync database URL (for Alembic)."""
        password = self.db_password.get_secret_value()
        return f"postgresql://{self.db_user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def redis_url(self) -> str:
        """Construct Redis URL."""
        if self.redis_password:
            password = self.redis_password.get_secret_value()
            return f"redis://:{password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
