{{ config(
    tags = ['daily']
) }}

with cnt_log as (
    SELECT * FROM {{ ref('int_log_cnt_event_type') }}
),

date_filtered as (
    SELECT * FROM cnt_log
    WHERE created_on BETWEEN DATE '{{ var('logical_previous_day') }}' - 30 AND DATE '{{ var('logical_previous_day') }}'
),

final as (
    SELECT 
        type,
        SUM(cnt_event_type) AS cnt_event_type
    FROM date_filtered
    GROUP BY type
)

SELECT * FROM final