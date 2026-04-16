{{ config(
    materialized='table',
    schema='ads',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['category'],
) }}

WITH category_daily AS (
    SELECT * FROM {{ ref('dws_category_daily') }}
),

category_summary AS (
    SELECT
        category,
        count(*) AS active_days,
        sum(total_interactions) AS total_interactions,
        sum(unique_users) AS total_users,
        sum(unique_items) AS total_items,
        sum(view_count) AS total_views,
        sum(cart_count) AS total_carts,
        sum(buy_count) AS total_buys,
        avg(conversion_rate) AS avg_conversion_rate,
        avg(avg_interactions_per_user) AS avg_interactions_per_user
    FROM category_daily
    GROUP BY category
),

total_buys AS (
    SELECT sum(total_buys) AS value FROM category_summary
)

SELECT
    cs.category,
    cs.active_days,
    cs.total_interactions,
    cs.total_users,
    cs.total_items,
    cs.total_views,
    cs.total_carts,
    cs.total_buys,
    round(cs.avg_conversion_rate, 2) AS avg_conversion_rate,
    round(cs.avg_interactions_per_user, 2) AS avg_interactions_per_user,
    row_number() OVER (ORDER BY cs.total_buys DESC) AS category_rank,
    round(cs.total_buys * 100.0 / nullIf((SELECT value FROM total_buys), 0), 2) AS market_share,
    now() AS updated_at
FROM category_summary cs
ORDER BY category_rank
