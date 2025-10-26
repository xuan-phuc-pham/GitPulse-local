{{ config(
    tags = ['daily']
) }}

with stg_events as (
    SELECT * FROM {{ ref('stg_events') }}
),

fil_cols_type_cast as (
    SELECT repo_id, created_at::date AS created_on FROM stg_events
),

cnt_event_by_repo as (
    SELECT repo_id, COUNT(*) AS cnt_event_repo 
    FROM fil_cols_type_cast
    WHERE created_on > DATE '{{ var('logical_previous_day') }}' - 7
    GROUP BY repo_id ORDER BY cnt_event_repo DESC
)


SELECT * FROM cnt_event_by_repo