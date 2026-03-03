# Econ-Bottleneck-GeoFin-Crits

A bottom-up economic bottleneck detection platform that ingests data from federal and state government sources, identifies supply chain and fiscal stress signals, and surfaces emerging risks through automated analysis.

## What It Does

The system collects macroeconomic, trade, commodity, labor, and fiscal data from ~20 government APIs and public datasets. It runs that data through a battery of domain-specific monitors (energy, shipping, minerals, labor, fiscal dominance, etc.) to detect bottlenecks, anomalies, and propagation risk across interconnected economic sectors.

**Core capabilities:**

- **Data ingestion** from FRED, BLS, BEA, EIA, Census, USDA NASS, SEC EDGAR, UN Comtrade, WARN Act state DOL sites, and others
- **Bottleneck detection** across 8 categories: energy, shipping, labor, commodities, fiscal, consumer stress, critical minerals, capacity utilization
- **Sector dependency mapping** with propagation simulation (how a shock in one sector cascades)
- **WARN Act tracking** across 40+ U.S. states with per-state HTML/PDF/CSV parsers
- **LLM-powered forecasting** for bottleneck duration and severity (optional, requires API key)
- **React dashboard** with D3 dependency graph, trade flow visualizations, and time-series explorer

## Requirements

- **Docker** and **Docker Compose** (primary deployment method)
- **Python 3.11+** (if running outside Docker)
- **Node.js 18+** (for the React frontend)
- **API keys** for government data sources (see `env.example` for the full list)

At minimum, you need FRED and BLS API keys for core functionality. Additional keys unlock more collectors.

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/Lildvs/Econ-Bottleneck-GeoFin-Crits.git
cd Econ-Bottleneck-GeoFin-Crits

# 2. Create your .env from the template
cp env.example .env
# Edit .env and add your API keys + set a DB password

# 3. Start all services (API, worker, TimescaleDB, Redis, Prometheus)
docker-compose up -d --build

# 4. Start the frontend
cd frontend-react
npm install
npm run dev
```

The API runs on `http://localhost:8000`, the frontend on `http://localhost:5173`.

## Architecture

| Component | Technology | Purpose |
|-----------|-----------|---------|
| API | FastAPI | REST endpoints for data, analysis, and detection |
| Worker | APScheduler | Scheduled data collection (boot, weekly, on-demand) |
| Database | TimescaleDB (PostgreSQL) | Time-series storage for all collected data |
| Cache | Redis | Rate limiting, response caching, pub/sub alerts |
| Frontend | React + Vite + D3 | Dashboard, sector graph, trade flows, WARN notices |
| Monitoring | Prometheus | Metrics collection |

## Project Structure

```
src/
  api/            # FastAPI routes and middleware
  analysis/       # Bottleneck detection, monitors, propagation engine
  config/         # Settings, sector definitions, data frequencies
  data_ingestion/ # Collectors, scheduler, rate limiter, research sources
  forecasting/    # LLM-based bottleneck forecasting
  processing/     # Data validation and normalization
  services/       # Alerts and notifications
  storage/        # TimescaleDB, Redis, archive management
frontend-react/   # React dashboard
scripts/          # Database init, backfill, API testing
tests/            # Unit and integration tests
```

## Data Sources

See `Data Sources.md` for the complete catalog of federal and state government data sources used by the platform.

## License

This project is provided as-is for educational and research purposes.
