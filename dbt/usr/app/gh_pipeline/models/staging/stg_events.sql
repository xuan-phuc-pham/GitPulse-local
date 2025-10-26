{{ config(
    tags = ['daily']
) }}

with source_data as (
    SELECT * FROM {{ source('src_gharchive_staging', 'raw_events') }}
),

filter_lim_2m_ago as (
    SELECT *
    FROM source_data
    WHERE created_at::date >= '{{ var("the_1st_2m_ago") }}'
),

final as (
    SELECT *
    FROM filter_lim_2m_ago
)

SELECT *
FROM final