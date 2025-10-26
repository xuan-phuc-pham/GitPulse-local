{{ config(
    materialized='incremental',
    unique_key=['type', 'created_on'],
    tags = ['daily']
) }}

with stg_events as (
    SELECT * FROM {{ ref('stg_events') }}
),

fil_cols_type_cast as (
    SELECT id, type, created_at::date AS created_on FROM stg_events
),

log_cnt_event_type as (
    SELECT type, 
    created_on,
    COUNT(*) AS cnt_event_type
    FROM fil_cols_type_cast
    WHERE created_on = '{{ var('logical_previous_day') }}'
    GROUP BY type, created_on
)

SELECT * FROM log_cnt_event_type