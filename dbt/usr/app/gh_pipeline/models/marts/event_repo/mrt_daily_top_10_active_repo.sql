{{ config(
    tags = ['daily']
) }}

with daily_active_repos as (
    SELECT * FROM {{ ref('int_yesterday_cnt_event_repo') }}
),

lim_10_final as (
    SELECT * FROM daily_active_repos LIMIT 10
)

SELECT * FROM lim_10_final
