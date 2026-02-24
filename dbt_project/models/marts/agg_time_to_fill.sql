WITH job_tracking AS (
    SELECT
        job_key,
        job_title,
        company_name,
        job_category,
        seniority_level,
        MIN(posted_date) AS first_posted_date,
        MAX(extracted_at)::DATE AS last_seen_date,
        CURRENT_DATE AS analysis_date
    FROM {{ ref('fct_job_postings') }}
    GROUP BY 1, 2, 3, 4, 5
),

lifecycle AS (
    SELECT
        *,
        (last_seen_date - first_posted_date) AS days_active,
        -- If we haven't seen this job in our daily scrapes for 3+ days, assume it's closed/filled
        CASE
            WHEN last_seen_date < analysis_date - INTERVAL '3 days' THEN TRUE
            ELSE FALSE
        END AS is_closed
    FROM job_tracking
),

cohort_metrics AS (
    SELECT
        job_category,
        seniority_level,
        COUNT(*) AS total_jobs_tracked,
        SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) AS total_closed_jobs,
        
        -- Time to fill metrics
        ROUND(AVG(CASE WHEN is_closed THEN days_active END), 1) AS avg_time_to_fill_days,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_closed THEN days_active END), 1) AS median_time_to_fill_days,
        
        -- Current active market
        SUM(CASE WHEN NOT is_closed THEN 1 ELSE 0 END) AS currently_active_jobs,
        ROUND(AVG(CASE WHEN NOT is_closed THEN days_active END), 1) AS avg_age_of_active_jobs
        
    FROM lifecycle
    GROUP BY 1, 2
)

SELECT * FROM cohort_metrics
ORDER BY total_jobs_tracked DESC
