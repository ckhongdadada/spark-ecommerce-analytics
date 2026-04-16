import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "superset-secret-key-change-me")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "DATABASE_URI",
    "postgresql+psycopg2://admin:admin123@postgres:5432/superset"
)

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": True,
}

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

SQLALCHEMY_CUSTOM_PASSWORD_STORE = lambda conn: conn.password

CLICKHOUSE_DRIVER = "clickhouse"
CLICKHOUSE_URI = "clickhouse+native://default:clickhouse123@clickhouse:9000/ecommerce"

MYSQL_URI = "mysql+pymysql://root:123456@mysql:3306/ecommerce"
