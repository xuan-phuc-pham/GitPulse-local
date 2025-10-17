with source_data as (
    SELECT * FROM {{ source('src_gharchive_staging', 'raw_orgs') }}
),

final as (
    SELECT *
    FROM source_data
)

SELECT *
FROM final