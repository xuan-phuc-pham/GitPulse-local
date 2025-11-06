{{ config(
    materialized='table',
    tags = ['daily']
) }}

with active_users as (
    SELECT * FROM {{ ref('int_30d_cnt_event_user') }}
),

user_info as (
    SELECT * FROM {{ ref('stg_users') }}
),

lim_10_final as (
    SELECT * 
    FROM active_users a, user_info b 
    WHERE a.actor_id = b.id
    ORDER BY cnt_event_user DESC
    LIMIT 10
)

SELECT * FROM lim_10_final

