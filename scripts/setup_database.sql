-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- The raw table is populated by pandas, but we can define it here if we want strict typing
-- CREATE TABLE IF NOT EXISTS raw.raw_job_postings (
--    source VARCHAR(50),
--    source_id VARCHAR(255),
--    title VARCHAR(500),
--    company VARCHAR(500),
--    location VARCHAR(500),
--    salary_min NUMERIC,
--    salary_max NUMERIC,
--    salary_currency VARCHAR(10),
--    description TEXT,
--    url TEXT,
--    posted_date TIMESTAMP,
--    extracted_at TIMESTAMP,
--    raw_data JSONB,
--    extraction_date DATE,
--    extraction_id VARCHAR(50)
-- );
