with monthly_active_users as (
    SELECT * FROM {{ ref('int_log_10_most_active_user_monthly') }}
),
lim_10_final as (
    SELECT * 
    FROM monthly_active_users 
    WHERE created_month = TO_CHAR(date_trunc('month', '{{ var("logical_previous_day") }}'::date) - interval '1 month','YYYY-MM') 
    LIMIT 10
)

SELECT * FROM lim_10_final
