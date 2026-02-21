{% macro extract_skills(column_name) %}
ARRAY(
    SELECT DISTINCT skill
    FROM UNNEST(ARRAY[
        CASE WHEN {{ column_name }} ILIKE '%sql%' THEN 'SQL' END,
        CASE WHEN {{ column_name }} ILIKE '%python%' THEN 'Python' END,
        CASE WHEN {{ column_name }} ILIKE '%tableau%' THEN 'Tableau' END,
        CASE WHEN {{ column_name }} ILIKE '%power bi%' OR {{ column_name }} ILIKE '%powerbi%' THEN 'Power BI' END,
        CASE WHEN {{ column_name }} ILIKE '%excel%' THEN 'Excel' END,
        CASE WHEN {{ column_name }} ILIKE '%r %' OR {{ column_name }} ILIKE '% r,' THEN 'R' END,
        CASE WHEN {{ column_name }} ILIKE '%looker%' THEN 'Looker' END,
        CASE WHEN {{ column_name }} ILIKE '%dbt%' THEN 'dbt' END,
        CASE WHEN {{ column_name }} ILIKE '%airflow%' THEN 'Airflow' END,
        CASE WHEN {{ column_name }} ILIKE '%spark%' THEN 'Spark' END,
        CASE WHEN {{ column_name }} ILIKE '%aws%' THEN 'AWS' END,
        CASE WHEN {{ column_name }} ILIKE '%gcp%' OR {{ column_name }} ILIKE '%google cloud%' THEN 'GCP' END,
        CASE WHEN {{ column_name }} ILIKE '%snowflake%' THEN 'Snowflake' END,
        CASE WHEN {{ column_name }} ILIKE '%redshift%' THEN 'Redshift' END,
        CASE WHEN {{ column_name }} ILIKE '%bigquery%' THEN 'BigQuery' END
    ]) AS skill
    WHERE skill IS NOT NULL
)
{% endmacro %}
