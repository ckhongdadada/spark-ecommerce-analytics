{{ config(
    materialized='table',
    schema='ads',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['user_id']
) }}

WITH user_daily AS (
    SELECT * FROM {{ ref('dws_user_daily') }}
),

user_profile AS (
    SELECT * FROM {{ ref('dwd_user_profile') }}
),

user_daily_summary AS (
    SELECT
        user_id,
        count(*) AS active_days,
        sum(total_behaviors) AS total_behaviors,
        sum(view_count) AS total_views,
        sum(cart_count) AS total_carts,
        sum(buy_count) AS total_buys,
        avg(conversion_rate) AS avg_conversion_rate,
        max(behavior_date) AS last_active_date
    FROM user_daily
    GROUP BY user_id
)

SELECT
    p.user_id,
    p.user_level,
    s.active_days,
    s.total_behaviors,
    s.total_views,
    s.total_carts,
    s.total_buys,
    round(s.avg_conversion_rate, 2) AS avg_conversion_rate,
    round(s.total_behaviors / nullIf(s.active_days, 0), 2) AS avg_behaviors_per_day,
    round((s.total_buys * 10 + s.total_carts * 5 + s.total_views) / 100.0, 2) AS user_value_score,
    s.last_active_date,
    now() AS updated_at
FROM user_profile p
LEFT JOIN user_daily_summary s ON p.user_id = s.user_id
