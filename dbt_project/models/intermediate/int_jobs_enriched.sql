WITH jobs AS (
    SELECT * FROM {{ ref('int_jobs_unioned') }}
),

skill_extraction AS (
    SELECT
        *,
        -- Extract skills from description
        {{ extract_skills('description_clean') }} AS skills_array,
        
        -- Categorize job level
        CASE
            WHEN job_title ILIKE '%senior%' OR job_title ILIKE '%sr%' THEN 'Senior'
            WHEN job_title ILIKE '%lead%' OR job_title ILIKE '%principal%' THEN 'Lead'
            WHEN job_title ILIKE '%manager%' OR job_title ILIKE '%director%' THEN 'Management'
            WHEN job_title ILIKE '%junior%' OR job_title ILIKE '%jr%' THEN 'Junior'
            WHEN job_title ILIKE '%intern%' THEN 'Intern'
            ELSE 'Mid-Level'
        END AS seniority_level,
        
        -- Categorize job type
        CASE
            WHEN job_title ILIKE '%data analyst%' THEN 'Data Analyst'
            WHEN job_title ILIKE '%business analyst%' THEN 'Business Analyst'
            WHEN job_title ILIKE '%data scientist%' THEN 'Data Scientist'
            WHEN job_title ILIKE '%analytics engineer%' THEN 'Analytics Engineer'
            WHEN job_title ILIKE '%bi %' OR job_title ILIKE '%business intelligence%' THEN 'BI Analyst'
            ELSE 'Other Analytics'
        END AS job_category,
        
        -- Calculate salary midpoint
        (COALESCE(salary_min_annual, 0) + COALESCE(salary_max_annual, 0)) / 
            NULLIF((CASE WHEN salary_min_annual IS NOT NULL THEN 1 ELSE 0 END + 
                    CASE WHEN salary_max_annual IS NOT NULL THEN 1 ELSE 0 END), 0) 
            AS salary_midpoint
            
    FROM jobs
)

SELECT * FROM skill_extraction
