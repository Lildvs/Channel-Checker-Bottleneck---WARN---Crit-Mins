-- Migration 002: Add WARN validation columns and scraper_health table
-- Supports government-first scraping with cross-validation against layoffdata.com

BEGIN;

-- Add source tracking and cross-validation columns to warn_notices
ALTER TABLE warn_notices
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(20) NOT NULL DEFAULT 'scraped',
    ADD COLUMN IF NOT EXISTS validation_status VARCHAR(30),
    ADD COLUMN IF NOT EXISTS validation_details JSONB,
    ADD COLUMN IF NOT EXISTS last_validated_at TIMESTAMPTZ;

-- Indexes for new columns
CREATE INDEX IF NOT EXISTS idx_warn_notices_data_source
    ON warn_notices (data_source);
CREATE INDEX IF NOT EXISTS idx_warn_notices_validation
    ON warn_notices (validation_status);

-- Scraper health tracking table
CREATE TABLE IF NOT EXISTS scraper_health (
    state VARCHAR(2) PRIMARY KEY,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    total_runs INTEGER NOT NULL DEFAULT 0,
    total_successes INTEGER NOT NULL DEFAULT 0,
    last_record_count INTEGER,
    status VARCHAR(20) NOT NULL DEFAULT 'healthy'
);

COMMIT;
