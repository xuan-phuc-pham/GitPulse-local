{{ config(
    tags = ['daily']
) }}

with stg_events as (
    SELECT * FROM {{ ref('stg_events') }}
),

fil_cols_type_cast as (
    SELECT actor_id, created_at::date AS created_on FROM stg_events
),

cnt_event_by_user as (
    SELECT actor_id, COUNT(*) AS cnt_event_user 
    FROM fil_cols_type_cast
    WHERE created_on > DATE '{{ var('logical_previous_day') }}' - 7
    GROUP BY actor_id ORDER BY cnt_event_user DESC
)


SELECT * FROM cnt_event_by_user