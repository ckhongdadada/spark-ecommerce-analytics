{{ config(
    materialized='incremental',
    schema='dwd',
    unique_key=['user_id', 'item_id', 'behavior_time'],
    engine='MergeTree()',
    order_by=['user_id', 'behavior_time'],
    partition_by=['toYYYYMM(behavior_date)']
) }}

WITH source AS (
    SELECT * FROM {{ ref('ods_user_behavior') }}
    {% if is_incremental() %}
    WHERE dt = '{{ var("etl_date") }}'
    {% endif %}
)

SELECT
    user_id,
    item_id,
    category,
    behavior,
    behavior_time,
    toDate(behavior_time) AS behavior_date,
    toHour(behavior_time) AS behavior_hour,
    toDayOfWeek(behavior_time) AS behavior_day_of_week,
    multiIf(
        behavior_day_of_week IN (1, 7), 1,
        0
    ) AS is_weekend,
    concat(user_id, '_', formatDateTime(behavior_time, '%Y%m%d%H')) AS session_id,
    dt
FROM source
