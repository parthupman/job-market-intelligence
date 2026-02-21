WITH jobs AS (
    SELECT * FROM {{ ref('fct_job_postings') }}
    WHERE salary_midpoint IS NOT NULL
),

by_role AS (
    SELECT
        job_category,
        seniority_level,
        
        COUNT(*) AS sample_size,
        
        ROUND(AVG(salary_midpoint), 0) AS avg_salary,
        ROUND(PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p10_salary,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p25_salary,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS median_salary,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p75_salary,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p90_salary,
        
        ROUND(STDDEV(salary_midpoint), 0) AS salary_stddev
        
    FROM jobs
    GROUP BY job_category, seniority_level
    HAVING COUNT(*) >= 10  -- Minimum sample size
)

SELECT * FROM by_role
ORDER BY job_category, 
    CASE seniority_level 
        WHEN 'Intern' THEN 1
        WHEN 'Junior' THEN 2
        WHEN 'Mid-Level' THEN 3
        WHEN 'Senior' THEN 4
        WHEN 'Lead' THEN 5
        WHEN 'Management' THEN 6
    END
