WITH adzuna AS (
    SELECT * FROM {{ ref('stg_adzuna_jobs') }}
),

remoteok AS (
    SELECT * FROM {{ ref('stg_remoteok_jobs') }}
),

themuse AS (
    SELECT * FROM {{ ref('stg_themuse_jobs') }}
),

unioned AS (
    SELECT * FROM adzuna
    UNION ALL
    SELECT * FROM remoteok
    UNION ALL
    SELECT * FROM themuse
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY job_title, company_name, location_raw
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM unioned
)

SELECT * FROM deduplicated WHERE row_num = 1
