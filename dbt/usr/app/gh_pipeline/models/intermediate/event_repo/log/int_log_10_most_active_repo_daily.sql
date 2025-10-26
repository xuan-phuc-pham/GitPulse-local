{{ config(
    materialized='incremental',
    tags = ['daily'],
    unique_key=['repo_id', 'created_on'],
) }}

with cnt_yesterday_event_repo as (
    SELECT * FROM {{ ref('int_yesterday_cnt_event_repo') }}
),

final as (
    SELECT * FROM cnt_yesterday_event_repo LIMIT 10
)

SELECT * FROM final