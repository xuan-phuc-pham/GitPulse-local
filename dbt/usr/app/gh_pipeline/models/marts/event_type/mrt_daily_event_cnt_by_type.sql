{{ config(
    tags = ['daily']
) }}

with cnt_log as (
    SELECT * FROM {{ ref('int_log_cnt_event_type') }}
),

date_filtered as (
    SELECT * FROM cnt_log
    WHERE created_on = '{{ var('logical_previous_day') }}'
),

final as (
    SELECT 
        type,
        cnt_event_type
    FROM date_filtered
)
SELECT * FROM final