"""FastAPI application for Channel Check Researcher API."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import analysis, bottlenecks, collectors, data, forecasts, reports, sectors, warn, research, trade_flows
from src.api.middleware.rate_limit import RateLimitMiddleware
from src.api.middleware.metrics import setup_metrics
from src.config.settings import get_settings
from src.storage.redis_cache import get_cache, init_cache
from src.storage.timescale import get_db, init_database

logger = structlog.get_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    # Startup -- configure structlog with DB persistence for the Reports tab
    from src.storage.log_handler import db_log_processor, start_log_flusher, stop_log_flusher, flush_log_buffer
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            db_log_processor,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    logger.info("Starting Channel Check Researcher API")

    try:
        await init_database()
        await init_cache()
        logger.info("Database and cache initialized")
    except Exception as e:
        logger.error("Failed to initialize connections", error=str(e))

    # Start the background log flusher (persists WARNING+ logs to DB for the Reports tab)
    await start_log_flusher(interval_seconds=5.0)

    # Pre-warm the bottleneck detection cache so the first dashboard load is instant
    import asyncio
    from src.analysis.bottleneck_detector import BottleneckDetector

    async def _prewarm_bottleneck_cache() -> None:
        try:
            db = get_db()
            cache = get_cache()
            detector = BottleneckDetector(db=db)
            signals = await detector.detect_all()
            result = [s.to_dict() for s in signals]
            await cache.cache_active_bottlenecks(result)
            logger.info("Bottleneck cache pre-warmed", signals=len(result))
        except Exception as exc:
            logger.warning("Cache pre-warm failed (will retry on first request)", error=str(exc))

    asyncio.create_task(_prewarm_bottleneck_cache())

    # Periodic cache refresh every 10 minutes (cache TTL is 15 min, so no gap)
    _refresh_running = True

    async def _periodic_bottleneck_refresh() -> None:
        while _refresh_running:
            await asyncio.sleep(600)  # 10 minutes
            try:
                db = get_db()
                cache = get_cache()
                detector = BottleneckDetector(db=db)
                signals = await detector.detect_all()
                result = [s.to_dict() for s in signals]
                await cache.cache_active_bottlenecks(result)
                logger.info("Bottleneck cache refreshed", signals=len(result))
            except Exception as exc:
                logger.warning("Periodic cache refresh failed", error=str(exc))

    asyncio.create_task(_periodic_bottleneck_refresh())

    yield

    _refresh_running = False

    logger.info("Shutting down API")

    try:
        await flush_log_buffer()
        stop_log_flusher()
    except Exception:
        pass

    try:
        db = get_db()
        await db.close()
        cache = get_cache()
        await cache.close()
    except Exception as e:
        logger.error("Error during shutdown", error=str(e))


app = FastAPI(
    title="Channel Check Researcher API",
    description=(
        "API for the Bottom-Up Fundamental Channel Check Researcher. "
        "Detects economic bottlenecks and their sector impacts in real-time."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8501",  # Streamlit (legacy)
        "http://localhost:3000",  # React (custom port)
        "http://localhost:5173",  # Vite dev server (default)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting middleware (must be added after CORS)
if settings.api_rate_limit_enabled:
    try:
        cache = get_cache()
        app.add_middleware(
            RateLimitMiddleware,
            redis_cache=cache,
            limit=settings.api_rate_limit,
            window=settings.api_rate_limit_window,
            enabled=settings.api_rate_limit_enabled,
        )
        logger.info(
            "Rate limiting enabled",
            limit=settings.api_rate_limit,
            window=settings.api_rate_limit_window,
        )
    except Exception as e:
        logger.warning("Failed to setup rate limiting middleware", error=str(e))

setup_metrics(app)

app.include_router(analysis.router, prefix="/api", tags=["Analysis"])
app.include_router(bottlenecks.router, prefix="/api/bottlenecks", tags=["Bottlenecks"])
app.include_router(sectors.router, prefix="/api/sectors", tags=["Sectors"])
app.include_router(data.router, prefix="/api/data", tags=["Data"])
app.include_router(forecasts.router, prefix="/api/forecasts", tags=["Forecasts"])
app.include_router(warn.router, prefix="/api", tags=["WARN Notices"])
app.include_router(research.router, prefix="/api", tags=["Research Papers"])
app.include_router(trade_flows.router, prefix="/api", tags=["Trade Flows"])
app.include_router(reports.router, prefix="/api", tags=["Reports"])
app.include_router(collectors.router, prefix="/api", tags=["Collectors"])


@app.get("/", tags=["Health"])
async def root() -> dict[str, str]:
    """Root endpoint - health check."""
    return {
        "status": "healthy",
        "service": "Channel Check Researcher API",
        "version": "0.1.0",
    }


@app.get("/health", tags=["Health"])
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/api/status", tags=["Health"])
async def api_status() -> dict[str, str | bool]:
    """Get detailed API status."""
    db_connected = False
    cache_connected = False

    try:
        db = get_db()
        async with db.session() as session:
            from sqlalchemy import text
            await session.execute(text("SELECT 1"))
        db_connected = True
    except Exception:
        pass

    try:
        cache = get_cache()
        await cache.client.ping()
        cache_connected = True
    except Exception:
        pass

    return {
        "status": "healthy" if db_connected and cache_connected else "degraded",
        "database": db_connected,
        "cache": cache_connected,
    }
