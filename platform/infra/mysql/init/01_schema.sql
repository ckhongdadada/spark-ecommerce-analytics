CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

CREATE TABLE IF NOT EXISTS user_behavior_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    item_id VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    behavior VARCHAR(16) NOT NULL COMMENT 'pv/buy/cart/fav',
    behavior_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dt DATE NOT NULL COMMENT 'partition date',
    INDEX idx_user_id (user_id),
    INDEX idx_item_id (item_id),
    INDEX idx_category (category),
    INDEX idx_behavior (behavior),
    INDEX idx_dt (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_profile (
    user_id VARCHAR(64) PRIMARY KEY,
    user_level VARCHAR(16) DEFAULT 'new',
    total_behaviors INT DEFAULT 0,
    total_views INT DEFAULT 0,
    total_carts INT DEFAULT 0,
    total_buys INT DEFAULT 0,
    total_favs INT DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0.00,
    first_active_time TIMESTAMP NULL,
    last_active_time TIMESTAMP NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS item_profile (
    item_id VARCHAR(64) PRIMARY KEY,
    category VARCHAR(64) NOT NULL,
    item_popularity VARCHAR(16) DEFAULT 'cold',
    total_interactions INT DEFAULT 0,
    unique_users INT DEFAULT 0,
    view_count INT DEFAULT 0,
    cart_count INT DEFAULT 0,
    buy_count INT DEFAULT 0,
    fav_count INT DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS order_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    item_id VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2) DEFAULT 0.00,
    status VARCHAR(16) DEFAULT 'created' COMMENT 'created/paid/shipped/completed/cancelled',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_order_id (order_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS category_daily_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(64) NOT NULL,
    stat_date DATE NOT NULL,
    total_interactions INT DEFAULT 0,
    unique_users INT DEFAULT 0,
    unique_items INT DEFAULT 0,
    view_count INT DEFAULT 0,
    cart_count INT DEFAULT 0,
    buy_count INT DEFAULT 0,
    fav_count INT DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_category_date (category, stat_date),
    INDEX idx_stat_date (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
