# Data Dictionary

This document outlines the final dimensional tables output by the `dbt` transformations, which power the visualizations in the dashboard.

## `fct_job_postings`
*The core fact table representing individual standardized job postings.*

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `job_key` | VARCHAR | Primary key surrogate hash |
| `source` | VARCHAR | Integration source (adzuna, remoteok, themuse) |
| `source_id` | VARCHAR | Original ID from the source API |
| `job_title` | VARCHAR | Cleaned and lowercased job title |
| `company_name` | VARCHAR | Extracted company name |
| `location_raw` | VARCHAR | Un-normalized location string |
| `seniority_level` | VARCHAR | Derived category: Junior, Mid-Level, Senior, Lead, Management |
| `job_category` | VARCHAR | Derived category: Data Analyst, Data Scientist, Analytics Engineer, etc. |
| `skills_array` | VARCHAR[] | Array of extracted skills (SQL, Python, dbt, etc.) |
| `salary_min_annual`| NUMERIC | Standardized annual minimum salary |
| `salary_max_annual`| NUMERIC | Standardized annual maximum salary |
| `salary_midpoint` | NUMERIC | Calculated midpoint of the salary range |
| `salary_currency` | VARCHAR | Currency code (default USD) |
| `description_clean`| TEXT | HTML-stripped job description |
| `job_url` | TEXT | Link to the original posting |
| `posted_date` | DATE | Date the job was originally posted |
| `extracted_at` | TIMESTAMP | Timestamp of when pipeline synced the job |
| `has_salary_info` | BOOLEAN | True if salary data is present |

## `agg_daily_metrics`
*Aggregate table showing market trends day over day.*

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `posted_date` | DATE | Date dimension |
| `total_jobs_posted` | INT | Count of unique listings that day |
| `unique_companies` | INT | Count of unique hiring companies |
| `median_salary` | NUMERIC | Overall median salary of the day |
| `data_analyst_count`| INT | Count of Data Analyst postings |
| `pct_with_salary` | NUMERIC | Percentage of jobs providing salary ranges |
| `jobs_7d_ma` | NUMERIC | 7-day moving average of job volume |

## `agg_salary_by_role`
*Aggregate table breaking down compensation statistics by role and seniority.*

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `job_category` | VARCHAR | Grouping dimension (e.g. Data Scientist) |
| `seniority_level` | VARCHAR | Grouping dimension (e.g. Senior) |
| `sample_size` | INT | Count of jobs in this bracket |
| `avg_salary` | NUMERIC | Arithmetic mean salary |
| `p25_salary` | NUMERIC | 25th percentile (lower quartile) |
| `median_salary` | NUMERIC | 50th percentile |
| `p75_salary` | NUMERIC | 75th percentile (upper quartile) |
