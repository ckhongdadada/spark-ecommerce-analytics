{{ config(materialized='view', schema='ods') }}

SELECT
    user_id,
    item_id,
    category,
    behavior,
    behavior_time,
    dt,
    load_time
FROM {{ source('ecommerce', 'ods_user_behavior') }}
WHERE behavior IN ('pv', 'buy', 'cart', 'fav')
  AND user_id IS NOT NULL
  AND item_id IS NOT NULL
