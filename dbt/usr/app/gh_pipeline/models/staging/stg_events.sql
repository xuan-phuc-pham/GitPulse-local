{{ config(
    tags = ['daily']
) }}

with source_data as (
    SELECT * FROM {{ source('src_gharchive_staging', 'raw_events') }}
),

final as (
    SELECT *
    FROM source_data
)

SELECT *
FROM final