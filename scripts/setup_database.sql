-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create user and grant privileges
-- Note: In production, use more restrictive permissions
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'jobmarket') THEN
      CREATE ROLE jobmarket LOGIN PASSWORD 'testpass';
   END IF;
END
$do$;

GRANT ALL PRIVILEGES ON DATABASE job_market TO jobmarket;
GRANT ALL PRIVILEGES ON SCHEMA raw TO jobmarket;
GRANT ALL PRIVILEGES ON SCHEMA staging TO jobmarket;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO jobmarket;
GRANT ALL PRIVILEGES ON SCHEMA marts TO jobmarket;

-- Optional: Create a specific role for dbt if needed later
-- CREATE ROLE dbt_user LOGIN PASSWORD 'dbt_pass';
-- GRANT USAGE ON SCHEMA raw TO dbt_user;
-- GRANT ALL PRIVILEGES ON SCHEMA staging TO dbt_user;
-- GRANT ALL PRIVILEGES ON SCHEMA intermediate TO dbt_user;
-- GRANT ALL PRIVILEGES ON SCHEMA marts TO dbt_user;
