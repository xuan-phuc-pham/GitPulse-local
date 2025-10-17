with daily_active_users as (
    SELECT * FROM {{ ref('int_yesterday_cnt_event_user') }}
),

lim_10_final as (
    SELECT * FROM daily_active_users LIMIT 10
)

SELECT * FROM lim_10_final
