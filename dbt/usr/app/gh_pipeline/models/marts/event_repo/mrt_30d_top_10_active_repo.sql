{{ config(
    tags = ['daily']
) }}

with active_users as (
    SELECT * FROM {{ ref('int_30d_cnt_event_repo') }}
),

lim_10_final as (
    SELECT * FROM active_users LIMIT 10
)

SELECT * FROM lim_10_final
