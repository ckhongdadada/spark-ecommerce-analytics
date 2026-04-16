{{ config(
    materialized='incremental',
    schema='dws',
    unique_key=['category', 'behavior_date'],
    engine='MergeTree()',
    order_by=['category', 'behavior_date'],
    partition_by=['toYYYYMM(behavior_date)']
) }}

WITH behavior_detail AS (
    SELECT * FROM {{ ref('dwd_user_behavior_detail') }}
    {% if is_incremental() %}
    WHERE dt = '{{ var("etl_date") }}'
    {% endif %}
)

SELECT
    category,
    behavior_date,
    count(*) AS total_interactions,
    count(DISTINCT user_id) AS unique_users,
    count(DISTINCT item_id) AS unique_items,
    count(DISTINCT session_id) AS session_count,
    sum(if(behavior = 'pv', 1, 0)) AS view_count,
    sum(if(behavior = 'cart', 1, 0)) AS cart_count,
    sum(if(behavior = 'buy', 1, 0)) AS buy_count,
    sum(if(behavior = 'fav', 1, 0)) AS fav_count,
    round(buy_count * 100.0 / nullIf(view_count, 0), 2) AS conversion_rate,
    round(total_interactions / nullIf(unique_users, 0), 2) AS avg_interactions_per_user
FROM behavior_detail
GROUP BY category, behavior_date
