{{ config(
    materialized='incremental',
    unique_key=['actor_id', 'created_month'],
    tags = ['monthly']
) }}

with stg_events as (
    SELECT * FROM {{ ref('stg_events') }}
),

fil_cols_type_cast as (
    SELECT actor_id, TO_CHAR(created_at, 'YYYY-MM') AS created_month FROM stg_events
),


top_10_most_active_actor as (
    SELECT actor_id, created_month, COUNT(*) AS cnt_event_actor 
    FROM fil_cols_type_cast
    WHERE created_month = TO_CHAR(date_trunc('month', '{{ var("logical_previous_day") }}'::date) - interval '1 month','YYYY-MM')
    GROUP BY actor_id, created_month ORDER BY cnt_event_actor DESC LIMIT 10
)

SELECT * FROM top_10_most_active_actor