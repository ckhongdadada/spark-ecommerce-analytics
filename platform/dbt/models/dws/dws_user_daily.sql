{{ config(
    materialized='incremental',
    schema='dws',
    unique_key=['user_id', 'behavior_date'],
    engine='MergeTree()',
    order_by=['user_id', 'behavior_date'],
    partition_by=['toYYYYMM(behavior_date)']
) }}

WITH behavior_detail AS (
    SELECT * FROM {{ ref('dwd_user_behavior_detail') }}
    {% if is_incremental() %}
    WHERE dt = '{{ var("etl_date") }}'
    {% endif %}
)

SELECT
    user_id,
    behavior_date,
    count(*) AS total_behaviors,
    count(DISTINCT item_id) AS unique_items,
    count(DISTINCT category) AS unique_categories,
    count(DISTINCT session_id) AS session_count,
    sum(if(behavior = 'pv', 1, 0)) AS view_count,
    sum(if(behavior = 'cart', 1, 0)) AS cart_count,
    sum(if(behavior = 'buy', 1, 0)) AS buy_count,
    sum(if(behavior = 'fav', 1, 0)) AS fav_count,
    min(behavior_hour) AS first_active_hour,
    max(behavior_hour) AS last_active_hour,
    last_active_hour - first_active_hour + 1 AS active_hours,
    round(buy_count * 100.0 / nullIf(view_count, 0), 2) AS conversion_rate
FROM behavior_detail
GROUP BY user_id, behavior_date
