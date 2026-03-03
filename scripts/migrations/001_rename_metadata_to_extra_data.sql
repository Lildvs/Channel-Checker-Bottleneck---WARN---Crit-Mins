-- Migration: Rename 'metadata' columns to 'extra_data'
-- Date: 2026-02-02
-- Reason: SQLAlchemy reserves 'metadata' as an internal ORM attribute.
--         Using 'metadata' as a column name causes InvalidRequestError at import time.
--
-- This migration is IDEMPOTENT: safe to run multiple times.
-- For fresh installs, this migration is not needed (init_db.sql already uses extra_data).

-- Transaction wrapper for atomicity
BEGIN;

-- Check if migration is needed (only proceed if 'metadata' column exists)
DO $$
BEGIN
    -- data_points table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'data_points' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE data_points RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed data_points.metadata to extra_data';
    END IF;

    -- sectors table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'sectors' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE sectors RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed sectors.metadata to extra_data';
    END IF;

    -- sector_dependencies table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'sector_dependencies' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE sector_dependencies RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed sector_dependencies.metadata to extra_data';
    END IF;

    -- series_metadata table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'series_metadata' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE series_metadata RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed series_metadata.metadata to extra_data';
    END IF;

    -- anomalies table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'anomalies' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE anomalies RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed anomalies.metadata to extra_data';
    END IF;

    -- forecasts table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'forecasts' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE forecasts RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed forecasts.metadata to extra_data';
    END IF;

    -- collection_jobs table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'collection_jobs' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE collection_jobs RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed collection_jobs.metadata to extra_data';
    END IF;

    -- research_signals table
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'research_signals' AND column_name = 'metadata'
    ) THEN
        ALTER TABLE research_signals RENAME COLUMN metadata TO extra_data;
        RAISE NOTICE 'Renamed research_signals.metadata to extra_data';
    END IF;

END $$;

COMMIT;

-- Verification query (run after migration)
-- SELECT table_name, column_name 
-- FROM information_schema.columns 
-- WHERE column_name IN ('metadata', 'extra_data') 
-- AND table_schema = 'public'
-- ORDER BY table_name;
