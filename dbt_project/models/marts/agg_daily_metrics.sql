WITH jobs AS (
    SELECT * FROM {{ ref('fct_job_postings') }}
),

daily_stats AS (
    SELECT
        posted_date,
        
        -- Volume metrics
        COUNT(*) AS total_jobs_posted,
        COUNT(DISTINCT company_name) AS unique_companies,
        
        -- Salary metrics
        ROUND(AVG(salary_midpoint), 0) AS avg_salary,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS median_salary,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p25_salary,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p75_salary,
        
        -- Category breakdown
        COUNT(*) FILTER (WHERE job_category = 'Data Analyst') AS data_analyst_count,
        COUNT(*) FILTER (WHERE job_category = 'Data Scientist') AS data_scientist_count,
        COUNT(*) FILTER (WHERE job_category = 'Business Analyst') AS business_analyst_count,
        
        -- Seniority breakdown
        COUNT(*) FILTER (WHERE seniority_level = 'Senior') AS senior_count,
        COUNT(*) FILTER (WHERE seniority_level = 'Mid-Level') AS mid_count,
        COUNT(*) FILTER (WHERE seniority_level = 'Junior') AS junior_count,
        
        -- Data quality
        COUNT(*) FILTER (WHERE has_salary_info) AS jobs_with_salary,
        ROUND(100.0 * COUNT(*) FILTER (WHERE has_salary_info) / COUNT(*), 1) AS pct_with_salary
        
    FROM jobs
    GROUP BY posted_date
)

SELECT 
    *,
    -- 7-day moving averages
    AVG(total_jobs_posted) OVER (ORDER BY posted_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS jobs_7d_ma,
    AVG(avg_salary) OVER (ORDER BY posted_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS salary_7d_ma
FROM daily_stats
ORDER BY posted_date DESC
