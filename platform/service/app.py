"""
电商数据平台 - FastAPI 服务层
提供 REST API 查询 ClickHouse 和 MySQL 中的数据
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
import os

from clickhouse_driver import Client
import pymysql


app = FastAPI(
    title="电商数据平台 API",
    description="电商全路径数据分析系统 REST API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_ch_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        database=os.getenv("CLICKHOUSE_DB", "ecommerce"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
    )


def get_mysql_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DB", "ecommerce"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "123456"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


class CategoryMetrics(BaseModel):
    category: str
    active_days: int
    total_interactions: int
    total_users: int
    total_items: int
    total_views: int
    total_carts: int
    total_buys: int
    avg_conversion_rate: float
    market_share: float
    category_rank: int


class UserMetrics(BaseModel):
    user_id: str
    user_level: str
    active_days: int
    total_behaviors: int
    total_views: int
    total_carts: int
    total_buys: int
    avg_conversion_rate: Optional[float]
    user_value_score: Optional[float]
    last_active_date: Optional[str]


class RealtimeMetrics(BaseModel):
    window_start: str
    window_end: str
    category: str
    total_events: int
    unique_users: int
    view_count: int
    cart_count: int
    buy_count: int
    fav_count: int
    conversion_rate: Optional[float]


class AlertEvent(BaseModel):
    alert_type: str
    alert_level: str
    category: str
    metric_name: str
    metric_value: float
    threshold: float
    alert_time: str
    description: str


class HealthResponse(BaseModel):
    status: str
    clickhouse: str
    mysql: str
    timestamp: str


@app.get("/health", response_model=HealthResponse)
def health_check():
    ch_status = "ok"
    mysql_status = "ok"
    try:
        ch = get_ch_client()
        ch.execute("SELECT 1")
    except Exception:
        ch_status = "error"
    try:
        conn = get_mysql_conn()
        conn.close()
    except Exception:
        mysql_status = "error"
    return HealthResponse(
        status="ok" if ch_status == "ok" and mysql_status == "ok" else "degraded",
        clickhouse=ch_status,
        mysql=mysql_status,
        timestamp=datetime.now().isoformat(),
    )


@app.get("/api/v1/category-metrics", response_model=List[CategoryMetrics])
def get_category_metrics():
    ch = get_ch_client()
    rows = ch.execute("""
        SELECT category, active_days, total_interactions, total_users,
               total_items, total_views, total_carts, total_buys,
               avg_conversion_rate, market_share, category_rank
        FROM ads_category_metrics FINAL
        ORDER BY category_rank
    """)
    return [CategoryMetrics(**dict(zip([
        "category", "active_days", "total_interactions", "total_users",
        "total_items", "total_views", "total_carts", "total_buys",
        "avg_conversion_rate", "market_share", "category_rank"
    ], row))) for row in rows]


@app.get("/api/v1/user-metrics/top", response_model=List[UserMetrics])
def get_top_users(limit: int = Query(default=20, ge=1, le=100)):
    ch = get_ch_client()
    rows = ch.execute(f"""
        SELECT user_id, user_level, active_days, total_behaviors,
               total_views, total_carts, total_buys,
               avg_conversion_rate, user_value_score, last_active_date
        FROM ads_user_metrics FINAL
        ORDER BY user_value_score DESC
        LIMIT {limit}
    """)
    return [UserMetrics(**dict(zip([
        "user_id", "user_level", "active_days", "total_behaviors",
        "total_views", "total_carts", "total_buys",
        "avg_conversion_rate", "user_value_score", "last_active_date"
    ], row))) for row in rows]


@app.get("/api/v1/user-metrics/{user_id}", response_model=UserMetrics)
def get_user_metrics(user_id: str):
    ch = get_ch_client()
    rows = ch.execute("""
        SELECT user_id, user_level, active_days, total_behaviors,
               total_views, total_carts, total_buys,
               avg_conversion_rate, user_value_score, last_active_date
        FROM ads_user_metrics FINAL
        WHERE user_id = %(user_id)s
    """, {"user_id": user_id})
    if not rows:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    row = rows[0]
    return UserMetrics(**dict(zip([
        "user_id", "user_level", "active_days", "total_behaviors",
        "total_views", "total_carts", "total_buys",
        "avg_conversion_rate", "user_value_score", "last_active_date"
    ], row)))


@app.get("/api/v1/realtime/metrics", response_model=List[RealtimeMetrics])
def get_realtime_metrics(
    category: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
):
    ch = get_ch_client()
    where = ""
    params = {}
    if category:
        where = "WHERE category = %(category)s"
        params["category"] = category
    rows = ch.execute(f"""
        SELECT window_start, window_end, category, total_events,
               unique_users, view_count, cart_count, buy_count,
               fav_count, conversion_rate
        FROM realtime_category_metrics
        {where}
        ORDER BY window_start DESC
        LIMIT {limit}
    """, params)
    return [RealtimeMetrics(**dict(zip([
        "window_start", "window_end", "category", "total_events",
        "unique_users", "view_count", "cart_count", "buy_count",
        "fav_count", "conversion_rate"
    ], row))) for row in rows]


@app.get("/api/v1/alerts", response_model=List[AlertEvent])
def get_alerts(
    level: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
):
    ch = get_ch_client()
    where = ""
    params = {}
    if level:
        where = "WHERE alert_level = %(level)s"
        params["level"] = level
    rows = ch.execute(f"""
        SELECT alert_type, alert_level, category, metric_name,
               metric_value, threshold, alert_time, description
        FROM alert_events
        {where}
        ORDER BY alert_time DESC
        LIMIT {limit}
    """, params)
    return [AlertEvent(**dict(zip([
        "alert_type", "alert_level", "category", "metric_name",
        "metric_value", "threshold", "alert_time", "description"
    ], row))) for row in rows]


@app.get("/api/v1/dws/user-daily")
def get_user_daily(
    user_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=500),
):
    ch = get_ch_client()
    conditions = []
    params = {}
    if user_id:
        conditions.append("user_id = %(user_id)s")
        params["user_id"] = user_id
    if start_date:
        conditions.append("behavior_date >= %(start_date)s")
        params["start_date"] = start_date
    if end_date:
        conditions.append("behavior_date <= %(end_date)s")
        params["end_date"] = end_date
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = ch.execute(f"""
        SELECT user_id, behavior_date, total_behaviors, unique_items,
               unique_categories, session_count, view_count, cart_count,
               buy_count, fav_count, first_active_hour, last_active_hour,
               active_hours, conversion_rate
        FROM dws_user_daily
        {where}
        ORDER BY behavior_date DESC
        LIMIT {limit}
    """, params)
    columns = [
        "user_id", "behavior_date", "total_behaviors", "unique_items",
        "unique_categories", "session_count", "view_count", "cart_count",
        "buy_count", "fav_count", "first_active_hour", "last_active_hour",
        "active_hours", "conversion_rate"
    ]
    return [dict(zip(columns, row)) for row in rows]


@app.get("/api/v1/dws/category-daily")
def get_category_daily(
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    ch = get_ch_client()
    conditions = []
    params = {}
    if category:
        conditions.append("category = %(category)s")
        params["category"] = category
    if start_date:
        conditions.append("behavior_date >= %(start_date)s")
        params["start_date"] = start_date
    if end_date:
        conditions.append("behavior_date <= %(end_date)s")
        params["end_date"] = end_date
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = ch.execute(f"""
        SELECT category, behavior_date, total_interactions, unique_users,
               unique_items, session_count, view_count, cart_count,
               buy_count, fav_count, conversion_rate, avg_interactions_per_user
        FROM dws_category_daily
        {where}
        ORDER BY behavior_date DESC
        LIMIT 200
    """, params)
    columns = [
        "category", "behavior_date", "total_interactions", "unique_users",
        "unique_items", "session_count", "view_count", "cart_count",
        "buy_count", "fav_count", "conversion_rate", "avg_interactions_per_user"
    ]
    return [dict(zip(columns, row)) for row in rows]


@app.get("/api/v1/funnel")
def get_funnel_analysis(category: Optional[str] = None):
    ch = get_ch_client()
    category_filter = "WHERE category = %(category)s" if category else ""
    params = {"category": category} if category else {}
    rows = ch.execute(f"""
        SELECT
            'pv' AS step_name,
            sum(view_count) AS count
        FROM dws_category_daily {category_filter}
        UNION ALL
        SELECT
            'fav' AS step_name,
            sum(fav_count) AS count
        FROM dws_category_daily {category_filter}
        UNION ALL
        SELECT
            'cart' AS step_name,
            sum(cart_count) AS count
        FROM dws_category_daily {category_filter}
        UNION ALL
        SELECT
            'buy' AS step_name,
            sum(buy_count) AS count
        FROM dws_category_daily {category_filter}
    """, params)
    steps = [dict(zip(["step_name", "count"], row)) for row in rows]
    if steps and steps[0]["count"] > 0:
        base = steps[0]["count"]
        for step in steps:
            step["conversion_rate"] = round(step["count"] / base * 100, 2) if base > 0 else 0
    return {"funnel": steps, "category": category}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
