"""LLM configuration for forecasting module.

This module provides configuration and factory functions for creating
LLM instances used in bottleneck forecasting.
"""

from dataclasses import dataclass
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class LLMConfig:
    """Configuration for an LLM model."""

    model: str
    temperature: float = 0.3
    timeout: int = 60
    max_tokens: int | None = None
    retry_attempts: int = 3


# Default LLM configurations for different forecasting purposes
LLM_CONFIGS: dict[str, LLMConfig] = {
    "default": LLMConfig(
        model="gpt-5",
        temperature=0.3,
        timeout=60,
    ),
    "research": LLMConfig(
        model="claude-3-5-sonnet-20241022",
        temperature=0.2,
        timeout=120,
    ),
    "analysis": LLMConfig(
        model="gpt-4o",
        temperature=0.2,
        timeout=90,
    ),
    "fallback": LLMConfig(
        model="gpt-4o-mini",
        temperature=0.3,
        timeout=30,
    ),
}


def get_llm_config(purpose: str = "default") -> LLMConfig:
    """Get LLM configuration for a specific purpose.

    Args:
        purpose: The purpose/use case for the LLM.
            Options: "default", "research", "analysis", "fallback"

    Returns:
        LLMConfig for the specified purpose
    """
    return LLM_CONFIGS.get(purpose, LLM_CONFIGS["default"])


def get_forecasting_llm(
    purpose: str = "default",
    override_model: str | None = None,
) -> Any:
    """Get a configured LLM instance for forecasting.

    This function wraps the forecasting-tools GeneralLlm with our
    configuration settings.

    Args:
        purpose: The purpose/use case for the LLM
        override_model: Optional model override

    Returns:
        GeneralLlm instance configured for forecasting

    Raises:
        ImportError: If forecasting-tools is not installed
    """
    try:
        from forecasting_tools.ai_models.general_llm import GeneralLlm
    except ImportError as e:
        logger.error(
            "forecasting-tools not installed",
            error=str(e),
        )
        raise ImportError(
            "forecasting-tools package is required for LLM forecasting. "
            "Ensure the package is installed from the Metaculus project."
        ) from e

    config = get_llm_config(purpose)
    model = override_model or config.model

    return GeneralLlm(
        model=model,
        temperature=config.temperature,
        timeout=config.timeout,
    )


def is_llm_available() -> bool:
    """Check if LLM forecasting is available.

    Returns:
        True if forecasting-tools is installed and API keys are configured
    """
    try:
        from forecasting_tools.ai_models.general_llm import GeneralLlm  # noqa: F401

        # Check for at least one API key
        from src.config.settings import get_settings

        settings = get_settings()

        has_openai = settings.openai_api_key is not None
        has_anthropic = settings.anthropic_api_key is not None
        has_openrouter = settings.openrouter_api_key is not None

        return has_openai or has_anthropic or has_openrouter

    except ImportError:
        return False
    except Exception as e:
        logger.warning("Error checking LLM availability", error=str(e))
        return False


def get_available_models() -> list[str]:
    """Get list of available LLM models based on configured API keys.

    Returns:
        List of model identifiers that can be used
    """
    from src.config.settings import get_settings

    settings = get_settings()
    models: list[str] = []

    if settings.openai_api_key:
        models.extend([
            "gpt-5",
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4-turbo",
            "o1-preview",
            "o1-mini",
        ])

    if settings.anthropic_api_key:
        models.extend([
            "claude-3-5-sonnet-20241022",
            "claude-3-opus-20240229",
            "claude-3-sonnet-20240229",
            "claude-3-haiku-20240307",
        ])

    if settings.openrouter_api_key:
        # OpenRouter provides access to many models
        models.extend([
            "openrouter/auto",  # Auto-select best model
            "openrouter/deepseek/deepseek-r1",
        ])

    return models


def estimate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> float:
    """Estimate the cost of an LLM call in USD.

    Args:
        model: Model identifier
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens

    Returns:
        Estimated cost in USD
    """
    # Pricing per 1M tokens (as of January 2026)
    pricing: dict[str, tuple[float, float]] = {
        # OpenAI (input, output per 1M tokens)
        "gpt-5": (5.00, 20.00),
        "gpt-4o": (2.50, 10.00),
        "gpt-4o-mini": (0.15, 0.60),
        "gpt-4-turbo": (10.00, 30.00),
        "o1-preview": (15.00, 60.00),
        "o1-mini": (3.00, 12.00),
        # Anthropic
        "claude-3-5-sonnet-20241022": (3.00, 15.00),
        "claude-3-opus-20240229": (15.00, 75.00),
        "claude-3-sonnet-20240229": (3.00, 15.00),
        "claude-3-haiku-20240307": (0.25, 1.25),
    }

    # Get pricing for model (default to gpt-5 pricing)
    input_price, output_price = pricing.get(model, (5.00, 20.00))

    # Calculate cost
    input_cost = (input_tokens / 1_000_000) * input_price
    output_cost = (output_tokens / 1_000_000) * output_price

    return input_cost + output_cost
