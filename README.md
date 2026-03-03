# Channel Checker Bottleneck-WARN-Crit Mins

A bottom-up economic bottleneck detection platform that ingests data from federal and state government sources, identifies supply chain and fiscal stress signals, and surfaces emerging risks through automated analysis.

## What It Does

The system collects macroeconomic, trade, commodity, labor, and fiscal data from ~20 government APIs and public datasets. It runs that data through a battery of domain-specific monitors (energy, shipping, minerals, labor, fiscal dominance, etc.) to detect bottlenecks, anomalies, and propagation risk across interconnected economic sectors.

**Core capabilities:**

- **Data ingestion** from FRED, BLS, BEA, EIA, Census, USDA NASS, SEC EDGAR, UN Comtrade, WARN Act state DOL sites, and others
- **Bottleneck detection** across 8 categories: energy, shipping, labor, commodities, fiscal, consumer stress, critical minerals, capacity utilization
- **Sector dependency mapping** with propagation simulation (how a shock in one sector cascades)
- **WARN Act tracking** across 40+ U.S. states with per-state HTML/PDF/CSV parsers
- **LLM-powered forecasting** for bottleneck duration and severity (optional, requires API key) (Still In Progress)
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

## Screenshots
<img width="898" height="725" alt="P1-1" src="https://github.com/user-attachments/assets/be98b888-2485-4763-978e-73b70c2b3ecf" />

<img width="903" height="718" alt="P1-2" src="https://github.com/user-attachments/assets/b7355131-f353-4df7-9f8b-66ed98a68e60" />

<img width="902" height="718" alt="P1-3" src="https://github.com/user-attachments/assets/fde0265e-90a1-4d70-9aa7-0a70066342f5" />

<img width="907" height="714" alt="P1-4" src="https://github.com/user-attachments/assets/8db154af-235e-47ed-938c-edcdac2498b2" />

img width="904" height="717" alt="P1-5" src="https://github.com/user-attachments/assets/7981ee24-77aa-4c61-8066-168335597a52" />

<img width="901" height="723" alt="P1-6" src="https://github.com/user-attachments/assets/8ebbfea8-63f0-4761-983c-de383887ecf1" />

<img width="898" height="720" alt="P1-7" src="https://github.com/user-attachments/assets/f0ea7fa5-1748-4e7b-b88c-320b7b92f18a" />
