WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_job_postings') }}
    WHERE source = 'remoteok'
),

cleaned AS (
    SELECT
        -- IDs
        {{ dbt_utils.generate_surrogate_key(['source', 'source_id']) }} AS job_key,
        source,
        source_id,
        
        -- Job details
        TRIM(LOWER(title)) AS job_title,
        TRIM(company) AS company_name,
        TRIM(location) AS location_raw,
        
        -- Salary
        CASE 
            WHEN salary_min < 1000 THEN salary_min * 2080  -- Hourly to annual
            ELSE salary_min 
        END AS salary_min_annual,
        CASE 
            WHEN salary_max < 1000 THEN salary_max * 2080
            ELSE salary_max 
        END AS salary_max_annual,
        COALESCE(salary_currency, 'USD') AS salary_currency,
        
        -- Clean description
        REGEXP_REPLACE(description, '<[^>]+>', '', 'g') AS description_clean,
        
        -- Dates
        posted_date::DATE AS posted_date,
        extracted_at::TIMESTAMP AS extracted_at,
        
        -- URL
        url AS job_url,
        
        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at
        
    FROM source
    WHERE 
        title IS NOT NULL
        AND posted_date >= CURRENT_DATE - INTERVAL '{{ var("extraction_lookback_days") }} days'
)

SELECT * FROM cleaned
