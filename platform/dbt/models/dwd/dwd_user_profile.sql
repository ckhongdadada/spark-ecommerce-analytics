{{ config(
    materialized='table',
    schema='dwd',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['user_id']
) }}

WITH source AS (
    SELECT * FROM {{ ref('ods_user_behavior') }}
)

SELECT
    user_id,
    min(behavior_time) AS first_behavior_time,
    max(behavior_time) AS last_behavior_time,
    count(*) AS total_behaviors,
    count(DISTINCT item_id) AS unique_items,
    count(DISTINCT category) AS unique_categories,
    sum(if(behavior = 'pv', 1, 0)) AS view_count,
    sum(if(behavior = 'cart', 1, 0)) AS cart_count,
    sum(if(behavior = 'buy', 1, 0)) AS buy_count,
    sum(if(behavior = 'fav', 1, 0)) AS fav_count,
    multiIf(
        total_behaviors >= 100, 'high_active',
        total_behaviors >= 50, 'medium_active',
        total_behaviors >= 10, 'low_active',
        'new_user'
    ) AS user_level,
    round(buy_count * 100.0 / nullIf(view_count, 0), 2) AS conversion_rate,
    now() AS updated_at
FROM source
GROUP BY user_id
