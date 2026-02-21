{{
    config(
        materialized='incremental',
        unique_key='job_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH enriched_jobs AS (
    SELECT * FROM {{ ref('int_jobs_enriched') }}
    {% if is_incremental() %}
    WHERE extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        -- Keys
        job_key,
        source,
        source_id,
        
        -- Dimensions (foreign keys)
        {{ dbt_utils.generate_surrogate_key(['company_name']) }} AS company_key,
        {{ dbt_utils.generate_surrogate_key(['location_raw']) }} AS location_key,
        
        -- Job attributes
        job_title,
        company_name,
        location_raw,
        seniority_level,
        job_category,
        skills_array,
        
        -- Salary
        salary_min_annual,
        salary_max_annual,
        salary_midpoint,
        salary_currency,
        
        -- Content
        description_clean,
        job_url,
        
        -- Dates
        posted_date,
        extracted_at,
        _loaded_at,
        
        -- Derived fields
        CURRENT_DATE - posted_date AS days_since_posted,
        CASE 
            WHEN salary_midpoint IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_salary_info
        
    FROM enriched_jobs
)

SELECT * FROM final
