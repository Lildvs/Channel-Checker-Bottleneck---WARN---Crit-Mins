-- Channel Check Researcher Database Schema
-- TimescaleDB initialization script

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Core data points table (will be converted to hypertable)
CREATE TABLE IF NOT EXISTS data_points (
    id UUID DEFAULT uuid_generate_v4(),
    source_id TEXT NOT NULL,
    series_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    value DOUBLE PRECISION,
    value_text TEXT,
    unit TEXT,
    quality_score DOUBLE PRECISION DEFAULT 1.0,
    is_preliminary BOOLEAN DEFAULT FALSE,
    revision_number INTEGER DEFAULT 0,
    extra_data JSONB DEFAULT '{}',
    PRIMARY KEY (id, timestamp)
);

-- Convert to hypertable
SELECT create_hypertable('data_points', 'timestamp', if_not_exists => TRUE);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_data_points_source_series 
    ON data_points (source_id, series_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_data_points_series_id 
    ON data_points (series_id, timestamp DESC);

-- Bottleneck signals table
CREATE TABLE IF NOT EXISTS bottleneck_signals (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    category TEXT NOT NULL,
    subcategory TEXT,
    severity DOUBLE PRECISION NOT NULL CHECK (severity >= 0 AND severity <= 1),
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    affected_sectors TEXT[] NOT NULL DEFAULT '{}',
    affected_commodities TEXT[] DEFAULT '{}',
    source_series TEXT[] DEFAULT '{}',
    evidence JSONB DEFAULT '{}',
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'resolved', 'false_positive', 'monitoring')),
    resolved_at TIMESTAMPTZ,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_bottleneck_signals_status 
    ON bottleneck_signals (status, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_bottleneck_signals_category 
    ON bottleneck_signals (category, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_bottleneck_signals_sectors 
    ON bottleneck_signals USING GIN (affected_sectors);

-- Sector definitions table
CREATE TABLE IF NOT EXISTS sectors (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    parent_code TEXT REFERENCES sectors(code),
    classification_system TEXT NOT NULL DEFAULT 'NAICS',
    extra_data JSONB DEFAULT '{}'
);

-- Sector dependency graph (from BEA I-O tables)
CREATE TABLE IF NOT EXISTS sector_dependencies (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    upstream_sector TEXT NOT NULL,
    downstream_sector TEXT NOT NULL,
    weight DOUBLE PRECISION NOT NULL CHECK (weight >= 0 AND weight <= 1),
    dependency_type TEXT DEFAULT 'supply',
    source TEXT DEFAULT 'BEA_IO',
    year INTEGER,
    extra_data JSONB DEFAULT '{}',
    UNIQUE (upstream_sector, downstream_sector, source, year)
);

CREATE INDEX IF NOT EXISTS idx_sector_deps_upstream 
    ON sector_dependencies (upstream_sector);
CREATE INDEX IF NOT EXISTS idx_sector_deps_downstream 
    ON sector_dependencies (downstream_sector);

-- Data series metadata
CREATE TABLE IF NOT EXISTS series_metadata (
    series_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    frequency TEXT,
    seasonal_adjustment TEXT,
    sector_codes TEXT[] DEFAULT '{}',
    last_updated TIMESTAMPTZ,
    extra_data JSONB DEFAULT '{}'
);

-- Anomalies detected
CREATE TABLE IF NOT EXISTS anomalies (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    series_id TEXT NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    anomaly_timestamp TIMESTAMPTZ NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity DOUBLE PRECISION NOT NULL,
    expected_value DOUBLE PRECISION,
    actual_value DOUBLE PRECISION,
    z_score DOUBLE PRECISION,
    detection_method TEXT NOT NULL,
    extra_data JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_anomalies_series 
    ON anomalies (series_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp 
    ON anomalies (anomaly_timestamp DESC);

-- Forecasts table
CREATE TABLE IF NOT EXISTS forecasts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    forecast_horizon_days INTEGER NOT NULL,
    prediction JSONB NOT NULL,
    confidence_interval JSONB,
    model_used TEXT,
    extra_data JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_forecasts_target 
    ON forecasts (target_type, target_id, created_at DESC);

-- Collection jobs tracking
CREATE TABLE IF NOT EXISTS collection_jobs (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    collector_name TEXT NOT NULL,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    records_collected INTEGER DEFAULT 0,
    error_message TEXT,
    extra_data JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_collection_jobs_collector 
    ON collection_jobs (collector_name, started_at DESC);

-- Create continuous aggregates for common queries
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_data_summary
WITH (timescaledb.continuous) AS
SELECT 
    series_id,
    source_id,
    time_bucket('1 day', timestamp) AS day,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS sample_count,
    MAX(quality_score) AS max_quality
FROM data_points
GROUP BY series_id, source_id, time_bucket('1 day', timestamp)
WITH NO DATA;

-- Refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('daily_data_summary',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- ============================================================================
-- COMPRESSION POLICY
-- Enable native TimescaleDB compression for efficient storage
-- Compresses chunks older than 7 days while keeping data fully queryable
-- ============================================================================

-- Enable compression on the data_points hypertable
ALTER TABLE data_points SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'source_id, series_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Auto-compress chunks older than 7 days
-- This typically achieves 90-95% compression ratio for time-series data
SELECT add_compression_policy('data_points', 
    compress_after => INTERVAL '7 days',
    if_not_exists => TRUE);

-- ============================================================================
-- RETENTION POLICY
-- Keep detailed data for 8 years, then automatically drop old chunks
-- This aligns with the archive retention policy for raw files
-- ============================================================================

-- Retention policy (keep data for 8 years)
SELECT add_retention_policy('data_points', INTERVAL '8 years', if_not_exists => TRUE);

-- Insert initial sector definitions (NAICS-based)
INSERT INTO sectors (code, name, description, classification_system) VALUES
    ('11', 'Agriculture, Forestry, Fishing and Hunting', 'Agriculture and related industries', 'NAICS'),
    ('21', 'Mining, Quarrying, and Oil and Gas Extraction', 'Extraction of natural resources', 'NAICS'),
    ('22', 'Utilities', 'Electric power, natural gas, water', 'NAICS'),
    ('23', 'Construction', 'Building construction and specialty trades', 'NAICS'),
    ('31-33', 'Manufacturing', 'All manufacturing industries', 'NAICS'),
    ('42', 'Wholesale Trade', 'Merchant wholesalers', 'NAICS'),
    ('44-45', 'Retail Trade', 'Retail stores and e-commerce', 'NAICS'),
    ('48-49', 'Transportation and Warehousing', 'Transportation and logistics', 'NAICS'),
    ('51', 'Information', 'Publishing, broadcasting, telecommunications', 'NAICS'),
    ('52', 'Finance and Insurance', 'Financial services and insurance', 'NAICS'),
    ('53', 'Real Estate and Rental and Leasing', 'Real estate activities', 'NAICS'),
    ('54', 'Professional, Scientific, and Technical Services', 'Professional services', 'NAICS'),
    ('55', 'Management of Companies and Enterprises', 'Holding companies', 'NAICS'),
    ('56', 'Administrative and Support Services', 'Administrative services', 'NAICS'),
    ('61', 'Educational Services', 'Schools and training', 'NAICS'),
    ('62', 'Health Care and Social Assistance', 'Healthcare and social services', 'NAICS'),
    ('71', 'Arts, Entertainment, and Recreation', 'Arts and recreation', 'NAICS'),
    ('72', 'Accommodation and Food Services', 'Hotels and restaurants', 'NAICS'),
    ('81', 'Other Services', 'Repair, personal services', 'NAICS'),
    ('92', 'Public Administration', 'Government', 'NAICS')
ON CONFLICT (code) DO NOTHING;

-- Custom sector groupings for bottleneck detection
INSERT INTO sectors (code, name, description, classification_system) VALUES
    ('ENERGY', 'Energy Sector', 'Oil, gas, electricity, renewables', 'CUSTOM'),
    ('MANUFACTURING', 'Manufacturing Sector', 'All manufacturing and industrial', 'CUSTOM'),
    ('AGRICULTURE', 'Agriculture Sector', 'Agriculture and food production', 'CUSTOM'),
    ('TRANSPORTATION', 'Transportation Sector', 'Transportation and logistics', 'CUSTOM'),
    ('TECHNOLOGY', 'Technology Sector', 'Technology and semiconductors', 'CUSTOM'),
    ('HOUSING', 'Housing Sector', 'Housing and construction', 'CUSTOM'),
    ('CONSUMER', 'Consumer Sector', 'Consumer and retail', 'CUSTOM'),
    ('HEALTHCARE', 'Healthcare Sector', 'Healthcare and pharmaceuticals', 'CUSTOM')
ON CONFLICT (code) DO NOTHING;

-- ============================================================================
-- RESEARCH INTELLIGENCE TABLES
-- ============================================================================

-- Research papers table
CREATE TABLE IF NOT EXISTS research_papers (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    doi TEXT UNIQUE,
    arxiv_id TEXT,
    title TEXT NOT NULL,
    abstract TEXT,
    authors TEXT[] DEFAULT '{}',
    institutions TEXT[] DEFAULT '{}',
    published_date TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    topics TEXT[] DEFAULT '{}',
    
    -- Validation scores
    quick_score DOUBLE PRECISION DEFAULT 0.5 CHECK (quick_score >= 0 AND quick_score <= 1),
    citation_count INTEGER DEFAULT 0,
    reference_count INTEGER DEFAULT 0,
    
    -- Contrarian classification
    research_type TEXT DEFAULT 'consensus' CHECK (research_type IN ('consensus', 'emerging', 'contrarian', 'low_quality_contrarian')),
    contrarian_confidence DOUBLE PRECISION DEFAULT 0.0,
    contradicts_papers TEXT[] DEFAULT '{}',
    
    -- Metadata
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    url TEXT NOT NULL,
    pdf_url TEXT,
    code_url TEXT,
    raw_metadata JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_research_papers_doi ON research_papers (doi);
CREATE INDEX IF NOT EXISTS idx_research_papers_arxiv ON research_papers (arxiv_id);
CREATE INDEX IF NOT EXISTS idx_research_papers_published ON research_papers (published_date DESC);
CREATE INDEX IF NOT EXISTS idx_research_papers_source ON research_papers (source, published_date DESC);
CREATE INDEX IF NOT EXISTS idx_research_papers_type ON research_papers (research_type, published_date DESC);
CREATE INDEX IF NOT EXISTS idx_research_papers_topics ON research_papers USING GIN (topics);

-- Research signals table (aggregated metrics)
CREATE TABLE IF NOT EXISTS research_signals (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    topic TEXT NOT NULL,
    
    -- Volume metrics
    paper_count INTEGER DEFAULT 0,
    new_paper_count INTEGER DEFAULT 0,
    citation_velocity DOUBLE PRECISION DEFAULT 0.0,
    
    -- Quality metrics
    avg_quick_score DOUBLE PRECISION DEFAULT 0.5,
    top_institution_ratio DOUBLE PRECISION DEFAULT 0.0,
    
    -- Trend signals
    contrarian_count INTEGER DEFAULT 0,
    emerging_count INTEGER DEFAULT 0,
    consensus_shift DOUBLE PRECISION DEFAULT 0.0,
    
    extra_data JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_research_signals_topic ON research_signals (topic, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_research_signals_timestamp ON research_signals (timestamp DESC);

-- Author cache table (for credibility lookups)
CREATE TABLE IF NOT EXISTS author_cache (
    author_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    h_index INTEGER,
    total_citations INTEGER DEFAULT 0,
    paper_count INTEGER DEFAULT 0,
    top_institution BOOLEAN DEFAULT FALSE,
    institution_name TEXT,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_author_cache_name ON author_cache (name);

-- ============================================================================
-- WARN ACT LAYOFF TRACKING TABLES
-- ============================================================================

-- WARN notices table (Worker Adjustment and Retraining Notification Act)
CREATE TABLE IF NOT EXISTS warn_notices (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    
    -- Company Information
    company_name TEXT NOT NULL,
    company_address TEXT,
    city TEXT,
    state TEXT NOT NULL,  -- State code (e.g., CA, TX)
    zip_code TEXT,
    county TEXT,
    
    -- Layoff Details
    notice_date TIMESTAMPTZ NOT NULL,
    effective_date TIMESTAMPTZ,
    layoff_date TIMESTAMPTZ,  -- Alias for effective_date in some states
    received_date TIMESTAMPTZ,  -- When state received the notice
    employees_affected INTEGER NOT NULL CHECK (employees_affected >= 0),
    layoff_type TEXT DEFAULT 'layoff' CHECK (layoff_type IN ('layoff', 'closure', 'relocation', 'furlough', 'unknown')),
    
    -- Industry Classification
    naics_code TEXT,
    naics_description TEXT,
    sector_category TEXT,  -- Our internal sector mapping
    
    -- Status and Notes
    is_temporary BOOLEAN DEFAULT FALSE,
    is_closure BOOLEAN DEFAULT FALSE,
    union_affected TEXT,
    reason TEXT,
    notes TEXT,
    
    -- Metadata
    source_state TEXT NOT NULL,  -- State that published notice
    source_url TEXT,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    raw_data JSONB DEFAULT '{}',
    
    -- Unique constraint to prevent duplicate notices
    UNIQUE (company_name, state, notice_date, employees_affected)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_warn_notices_state ON warn_notices (state, notice_date DESC);
CREATE INDEX IF NOT EXISTS idx_warn_notices_date ON warn_notices (notice_date DESC);
CREATE INDEX IF NOT EXISTS idx_warn_notices_sector ON warn_notices (sector_category, notice_date DESC);
CREATE INDEX IF NOT EXISTS idx_warn_notices_naics ON warn_notices (naics_code);
CREATE INDEX IF NOT EXISTS idx_warn_notices_company ON warn_notices (company_name);
CREATE INDEX IF NOT EXISTS idx_warn_notices_collected ON warn_notices (collected_at DESC);

-- Continuous aggregate for weekly layoff rollups
CREATE MATERIALIZED VIEW IF NOT EXISTS warn_weekly_summary
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', notice_date) AS week,
    state,
    sector_category,
    COUNT(*) AS notice_count,
    SUM(employees_affected) AS total_employees,
    AVG(employees_affected) AS avg_employees_per_notice,
    COUNT(CASE WHEN is_closure THEN 1 END) AS closure_count,
    COUNT(CASE WHEN is_temporary THEN 1 END) AS temporary_count
FROM warn_notices
GROUP BY time_bucket('1 week', notice_date), state, sector_category
WITH NO DATA;

-- Note: Continuous aggregate policy will be added once we have data
-- SELECT add_continuous_aggregate_policy('warn_weekly_summary',
--     start_offset => INTERVAL '4 weeks',
--     end_offset => INTERVAL '1 day',
--     schedule_interval => INTERVAL '1 day',
--     if_not_exists => TRUE);

-- Monthly rollup view for trend analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS warn_monthly_summary
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', notice_date) AS month,
    state,
    sector_category,
    naics_code,
    COUNT(*) AS notice_count,
    SUM(employees_affected) AS total_employees,
    AVG(employees_affected) AS avg_employees_per_notice,
    COUNT(CASE WHEN is_closure THEN 1 END) AS closure_count,
    COUNT(CASE WHEN is_temporary THEN 1 END) AS temporary_count,
    COUNT(DISTINCT company_name) AS unique_companies
FROM warn_notices
GROUP BY time_bucket('1 month', notice_date), state, sector_category, naics_code
WITH NO DATA;

-- ============================================================================
-- BEA INPUT-OUTPUT TABLES
-- Inter-industry dependency coefficients from Bureau of Economic Analysis
-- ============================================================================

-- I-O Coefficients table (stores direct requirements, total requirements,
-- and Make/Use/Supply/Import matrices from BEA)
CREATE TABLE IF NOT EXISTS io_coefficients (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    year INTEGER NOT NULL,
    table_type TEXT NOT NULL,  -- 'direct_requirements', 'total_requirements', 'make', 'use', 'supply', 'import_matrix'
    detail_level TEXT NOT NULL,  -- 'sector' (15 industries), 'summary' (71 industries), 'detail' (402)
    from_industry TEXT NOT NULL,  -- BEA industry code (source of input)
    from_industry_name TEXT,
    to_industry TEXT NOT NULL,  -- BEA industry code (consumer of input)
    to_industry_name TEXT,
    coefficient DECIMAL(12, 10) NOT NULL,  -- The I-O coefficient value
    commodity_code TEXT,  -- BEA commodity code (for Make/Use/Supply tables)
    commodity_name TEXT,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint for upserts
    UNIQUE(year, table_type, detail_level, from_industry, to_industry)
);

-- Indexes for I-O coefficient queries
CREATE INDEX IF NOT EXISTS idx_io_coef_year_type ON io_coefficients(year, table_type);
CREATE INDEX IF NOT EXISTS idx_io_coef_from ON io_coefficients(from_industry);
CREATE INDEX IF NOT EXISTS idx_io_coef_to ON io_coefficients(to_industry);
CREATE INDEX IF NOT EXISTS idx_io_coef_detail ON io_coefficients(detail_level, year);
CREATE INDEX IF NOT EXISTS idx_io_coef_commodity ON io_coefficients(commodity_code);

-- Check constraints
ALTER TABLE io_coefficients ADD CONSTRAINT check_table_type 
    CHECK (table_type IN ('direct_requirements', 'total_requirements', 'make', 'use', 'supply', 'import_matrix'));
ALTER TABLE io_coefficients ADD CONSTRAINT check_detail_level 
    CHECK (detail_level IN ('sector', 'summary', 'detail'));

-- Grant permissions (for application user)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO channelcheck;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO channelcheck;
