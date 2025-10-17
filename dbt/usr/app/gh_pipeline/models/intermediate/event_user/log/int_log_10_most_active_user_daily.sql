{{ config(
    materialized='incremental',
    unique_key=['actor_id', 'created_on']
) }}

with cnt_yesterday_event_user as (
    SELECT * FROM {{ ref('int_yesterday_cnt_event_user') }}
),

final as (
    SELECT * FROM cnt_yesterday_event_user LIMIT 10
)

SELECT * FROM final