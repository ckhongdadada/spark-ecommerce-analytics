{{ config(
    materialized='table',
    schema='dwd',
    engine='ReplacingMergeTree(updated_at)',
    order_by=['item_id'],
    partition_by=['category']
) }}

WITH source AS (
    SELECT * FROM {{ ref('ods_user_behavior') }}
)

SELECT
    item_id,
    category,
    count(*) AS total_interactions,
    count(DISTINCT user_id) AS unique_users,
    sum(if(behavior = 'pv', 1, 0)) AS view_count,
    sum(if(behavior = 'cart', 1, 0)) AS cart_count,
    sum(if(behavior = 'buy', 1, 0)) AS buy_count,
    sum(if(behavior = 'fav', 1, 0)) AS fav_count,
    multiIf(
        unique_users >= 100, 'hot',
        unique_users >= 50, 'warm',
        unique_users >= 10, 'normal',
        'cold'
    ) AS item_popularity,
    round(buy_count * 100.0 / nullIf(view_count, 0), 2) AS conversion_rate,
    now() AS updated_at
FROM source
GROUP BY item_id, category
