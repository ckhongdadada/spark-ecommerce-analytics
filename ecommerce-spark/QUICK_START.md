# 快速开始指南

## 项目增强功能概览

本项目实现了三大核心增强：
1. **MySQL Sink** - 数据下沉到 MySQL 数据库
2. **Delta Lake** - 现代数据湖支持
3. **漏斗分析** - 多维度用户行为转化分析

## 分支选择

### feature/mysql-sink 分支
- 功能：MySQL 数据下沉
- 适用场景：需要将分析结果写入 MySQL 供 BI 工具使用
- 不包含：Delta Lake、漏斗分析

### feature/delta-lake 分支 ⭐ 推荐
- 功能：MySQL Sink + Delta Lake + 漏斗分析
- 适用场景：完整的数据分析系统
- 包含所有增强功能

## 5 分钟快速体验

### 步骤 1: 切换到推荐分支

```bash
git checkout feature/delta-lake
```

### 步骤 2: 构建项目

```bash
cd ecommerce-spark
.\.build-env\apache-maven-3.9.9\bin\mvn.cmd clean package -DskipTests
```

### 步骤 3: 运行漏斗分析

```bash
# 使用 Parquet 格式（默认）
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2

# 或使用 Delta 格式
export DATA_FORMAT=delta
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2
```

### 步骤 4: 查看结果

#### 查看 ASCII 漏斗图
```bash
cat output/sql_report/funnel_analysis/visualization/funnel_ascii.txt
```

#### 查看数据文件
```bash
# 查看漏斗分析结果
ls output/sql_report/funnel_analysis/

# 查看 Delta Lake 事务日志（如果使用 Delta 格式）
ls output/sql_report/funnel_analysis/funnel_global/_delta_log
```

## 配置 MySQL（可选）

如果需要将结果写入 MySQL：

### 步骤 1: 创建数据库和表

```sql
CREATE DATABASE ecommerce;
USE ecommerce;

-- 基础报表
CREATE TABLE report_funnel (
    category VARCHAR(50),
    pv_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    conv_rate_pct DECIMAL(5,2)
);

CREATE TABLE report_hour_trend (
    hour INT,
    pv BIGINT,
    uv BIGINT
);

CREATE TABLE report_top3_items (
    category VARCHAR(50),
    itemId VARCHAR(50),
    buy_cnt BIGINT,
    rk INT
);

-- 流式指标
CREATE TABLE stream_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    behavior VARCHAR(20),
    pv BIGINT,
    uv BIGINT
);

-- 漏斗分析表
CREATE TABLE funnel_global (
    dimension VARCHAR(50),
    step1_exposure_users BIGINT,
    step2_click_users BIGINT,
    step3_cart_users BIGINT,
    step4_buy_users BIGINT,
    step1_exposure_events BIGINT,
    step2_click_events BIGINT,
    step3_cart_events BIGINT,
    step4_buy_events BIGINT,
    step1_to_step2_rate DECIMAL(5,2),
    step2_to_step3_rate DECIMAL(5,2),
    step3_to_step4_rate DECIMAL(5,2),
    overall_conversion_rate DECIMAL(5,2),
    step1_to_step2_loss_rate DECIMAL(5,2),
    step2_to_step3_loss_rate DECIMAL(5,2),
    step3_to_step4_loss_rate DECIMAL(5,2)
);

-- 其他漏斗表（结构相同）
CREATE TABLE funnel_category LIKE funnel_global;
CREATE TABLE funnel_hourly LIKE funnel_global;
CREATE TABLE funnel_user_segment LIKE funnel_global;

CREATE TABLE funnel_loss_analysis (
    loss_stage VARCHAR(100),
    loss_users BIGINT,
    loss_rate DECIMAL(5,2)
);
```

### 步骤 2: 配置连接

```bash
export MYSQL_URL="jdbc:mysql://localhost:3306/ecommerce?useSSL=false&serverTimezone=UTC"
export MYSQL_USER="root"
export MYSQL_PASSWORD="your_password"
```

### 步骤 3: 运行并写入 MySQL

```bash
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2
```

### 步骤 4: 查询 MySQL 结果

```sql
-- 查看全局漏斗
SELECT * FROM funnel_global;

-- 查看流失分析
SELECT * FROM funnel_loss_analysis ORDER BY loss_users DESC;

-- 查看分类漏斗
SELECT dimension, overall_conversion_rate 
FROM funnel_category 
ORDER BY overall_conversion_rate DESC;
```

## 核心功能演示

### 1. 漏斗分析输出示例

运行 Module2 后，控制台会显示：

```
====================================================================================================
  用户行为漏斗分析 - Global
====================================================================================================

┌────────────────────────────────────────────────────────────────────────────────┐
│                        Step 1: 曝光 (10,000 用户)                              │
└────────────────────────────────────────────────────────────────────────────────┘
                                  ↓ 转化率: 85.50%

                    ┌──────────────────────────────────────────────┐
                    │            Step 2: 点击 (8,550 用户)          │
                    └──────────────────────────────────────────────┘
                                  ↓ 转化率: 35.20%

                            ┌──────────────────────────┐
                            │  Step 3: 加购 (3,010 用户) │
                            └──────────────────────────┘
                                  ↓ 转化率: 45.80%

                                ┌──────────────┐
                                │ Step 4: 支付  │
                                │ (1,379 用户)  │
                                └──────────────┘

====================================================================================================
  整体转化率: 13.79%
====================================================================================================
```

### 2. 流失分析输出示例

```
+---------------------------+-----------+-----------+
|loss_stage                 |loss_users |loss_rate  |
+---------------------------+-----------+-----------+
|Step2->Step3: 点击未加购    |5,540      |64.80      |
|Step3->Step4: 加购未支付    |1,631      |54.20      |
|Step1->Step2: 曝光未点击    |1,450      |14.50      |
+---------------------------+-----------+-----------+
```

### 3. 分类漏斗对比

```
+-----------+------------------------+
|dimension  |overall_conversion_rate |
+-----------+------------------------+
|Electronics|15.23                   |
|Clothing   |14.56                   |
|Books      |13.89                   |
|Food       |12.34                   |
|Sports     |11.78                   |
|Beauty     |10.92                   |
+-----------+------------------------+
```

## 配置选项

### 数据格式选择

```bash
# 使用 Parquet（默认）
export DATA_FORMAT=parquet

# 使用 Delta Lake
export DATA_FORMAT=delta
```

### 流式 Sink 选择

```bash
# 文件输出（默认）
export STREAM_SINK=file

# MySQL 输出
export STREAM_SINK=mysql

# Kafka 输出
export STREAM_SINK=kafka
```

### MySQL 配置

```bash
export MYSQL_URL="jdbc:mysql://localhost:3306/ecommerce"
export MYSQL_USER="root"
export MYSQL_PASSWORD="password"
export MYSQL_BATCH_SIZE=1000
export MYSQL_PARTITIONS=4
```

## 输出目录结构

```
output/
├── sql_report/
│   ├── funnel_report/              # 原有漏斗报表
│   ├── hour_trend/                 # 小时趋势
│   ├── top3_items/                 # Top3 商品
│   └── funnel_analysis/            # 新增：漏斗分析
│       ├── funnel_global/          # 全局漏斗
│       ├── funnel_category/        # 分类漏斗
│       ├── funnel_hourly/          # 时段漏斗
│       ├── funnel_user_segment/    # 用户分群漏斗
│       ├── funnel_loss_analysis/   # 流失分析
│       └── visualization/          # 可视化配置
│           ├── funnel_ascii.txt
│           └── funnel_echarts_*.json
├── streaming/                      # 流式输出
├── benchmark/                      # 性能测试
└── cleaned/                        # 清洗后数据
```

## 常见问题

### Q1: 构建失败怎么办？
```bash
# 清理后重新构建
.\.build-env\apache-maven-3.9.9\bin\mvn.cmd clean
.\.build-env\apache-maven-3.9.9\bin\mvn.cmd package -DskipTests
```

### Q2: MySQL 连接失败？
- 检查 MySQL 服务是否运行
- 验证连接字符串、用户名、密码
- 确认数据库和表已创建
- 注意：MySQL 连接失败不会中断程序，只会跳过 MySQL 写入

### Q3: Delta Lake 报错？
- Windows 环境需要配置 HADOOP_HOME
- 或者使用默认的 Parquet 格式
- 详见 `DELTA_LAKE_SETUP.md`

### Q4: 如何只运行漏斗分析？
漏斗分析已集成到 Module2，运行 Module2 即可：
```bash
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2
```

## 下一步

### 查看详细文档
- [Delta Lake 使用指南](DELTA_LAKE_SETUP.md)
- [漏斗分析详细文档](FUNNEL_ANALYSIS_GUIDE.md)
- [项目增强总结](PROJECT_ENHANCEMENTS.md)

### 探索更多功能
```bash
# 运行所有模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 运行流式处理（需要 Kafka 或 Socket）
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 3

# 运行性能测试
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 6
```

### 自定义开发
参考 `FunnelAnalysis.scala` 和 `FunnelVisualization.scala` 添加自定义分析维度。

## 技术支持

如有问题，请查看：
1. 项目 README
2. 详细文档（DELTA_LAKE_SETUP.md, FUNNEL_ANALYSIS_GUIDE.md）
3. Git 提交历史和代码注释

---

**祝使用愉快！** 🎉
