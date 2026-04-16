CREATE DATABASE IF NOT EXISTS ecommerce;

-- ==================== 实时指标表 ====================

CREATE TABLE IF NOT EXISTS ecommerce.realtime_category_metrics
(
    window_start DateTime,
    window_end DateTime,
    category String,
    total_events UInt64,
    unique_users UInt64,
    view_count UInt64,
    cart_count UInt64,
    buy_count UInt64,
    fav_count UInt64,
    conversion_rate Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (category, window_start)
TTL window_start + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ecommerce.realtime_order_metrics
(
    window_start DateTime,
    window_end DateTime,
    category String,
    order_count UInt64,
    total_amount Decimal(20, 2),
    unique_buyers UInt64,
    avg_order_amount Decimal(20, 2),
    completed_count UInt64,
    cancelled_count UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (category, window_start)
TTL window_start + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ==================== 数仓 ADS 层镜像表 ====================

CREATE TABLE IF NOT EXISTS ecommerce.ads_user_metrics
(
    user_id String,
    user_level String,
    active_days UInt64,
    total_behaviors UInt64,
    total_views UInt64,
    total_carts UInt64,
    total_buys UInt64,
    avg_conversion_rate Float64,
    avg_behaviors_per_day Float64,
    user_value_score Float64,
    last_active_date Date,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ecommerce.ads_item_metrics
(
    item_id String,
    category String,
    item_popularity String,
    active_days UInt64,
    total_interactions UInt64,
    total_users UInt64,
    total_views UInt64,
    total_carts UInt64,
    total_buys UInt64,
    avg_conversion_rate Float64,
    avg_interactions_per_day Float64,
    item_heat_score Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY category
ORDER BY (item_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ecommerce.ads_category_metrics
(
    category String,
    active_days UInt64,
    total_interactions UInt64,
    total_users UInt64,
    total_items UInt64,
    total_views UInt64,
    total_carts UInt64,
    total_buys UInt64,
    avg_conversion_rate Float64,
    avg_interactions_per_user Float64,
    category_rank UInt64,
    market_share Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (category)
SETTINGS index_granularity = 8192;

-- ==================== DWS 层镜像表 ====================

CREATE TABLE IF NOT EXISTS ecommerce.dws_user_daily
(
    user_id String,
    behavior_date Date,
    total_behaviors UInt64,
    unique_items UInt64,
    unique_categories UInt64,
    session_count UInt64,
    view_count UInt64,
    cart_count UInt64,
    buy_count UInt64,
    fav_count UInt64,
    first_active_hour UInt8,
    last_active_hour UInt8,
    active_hours UInt8,
    conversion_rate Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(behavior_date)
ORDER BY (user_id, behavior_date)
TTL behavior_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ecommerce.dws_item_daily
(
    item_id String,
    category String,
    behavior_date Date,
    total_interactions UInt64,
    unique_users UInt64,
    session_count UInt64,
    view_count UInt64,
    cart_count UInt64,
    buy_count UInt64,
    fav_count UInt64,
    conversion_rate Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(behavior_date)
ORDER BY (item_id, behavior_date)
TTL behavior_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ecommerce.dws_category_daily
(
    category String,
    behavior_date Date,
    total_interactions UInt64,
    unique_users UInt64,
    unique_items UInt64,
    session_count UInt64,
    view_count UInt64,
    cart_count UInt64,
    buy_count UInt64,
    fav_count UInt64,
    conversion_rate Float64,
    avg_interactions_per_user Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(behavior_date)
ORDER BY (category, behavior_date)
TTL behavior_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ==================== 物化视图：实时汇总 ====================

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_category_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour_start)
ORDER BY (category, hour_start)
AS SELECT
    toStartOfHour(window_start) AS hour_start,
    category,
    sum(total_events) AS total_events,
    sum(unique_users) AS unique_users,
    sum(view_count) AS view_count,
    sum(cart_count) AS cart_count,
    sum(buy_count) AS buy_count,
    sum(fav_count) AS fav_count
FROM ecommerce.realtime_category_metrics
GROUP BY hour_start, category;

-- ==================== 告警事件表 ====================

CREATE TABLE IF NOT EXISTS ecommerce.alert_events
(
    alert_type String,
    alert_level String,
    category String,
    metric_name String,
    metric_value Float64,
    threshold Float64,
    alert_time DateTime,
    description String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(alert_time)
ORDER BY (alert_level, alert_time)
TTL alert_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
