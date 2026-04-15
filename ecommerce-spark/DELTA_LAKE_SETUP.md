# Delta Lake 与 MySQL Sink 功能使用指南

## 分支说明

### feature/mysql-sink 分支
包含 MySQL 数据下沉功能，支持将所有分析结果写入 MySQL 数据库。

**功能特性：**
- Module2 的三个报表（漏斗、小时趋势、Top3）全部写入 MySQL
- Module3 流式指标支持 MySQL sink（通过 foreachBatch）
- 配置驱动的 MySQL 连接参数

**切换到此分支：**
```bash
git checkout feature/mysql-sink
```

### feature/delta-lake 分支
基于 feature/mysql-sink，增加了 Delta Lake 支持。

**功能特性：**
- 包含 feature/mysql-sink 的所有功能
- 支持 Delta Lake 格式存储（ACID 事务）
- 通过配置切换 Parquet/Delta 格式
- SparkSession 自动配置 Delta 扩展

**切换到此分支：**
```bash
git checkout feature/delta-lake
```

## 使用方式

### 1. MySQL Sink 配置

在 `AppConfig.scala` 或通过环境变量配置：

```bash
# MySQL 连接配置
export MYSQL_URL="jdbc:mysql://localhost:3306/ecommerce?useSSL=false&serverTimezone=UTC"
export MYSQL_USER="root"
export MYSQL_PASSWORD="your_password"

# 表名配置（已有默认值）
export MYSQL_TABLE_FUNNEL="report_funnel"
export MYSQL_TABLE_TREND="report_hour_trend"
export MYSQL_TABLE_TOP3="report_top3_items"
export MYSQL_TABLE_STREAM_METRICS="stream_metrics"

# 流式 sink 选择
export STREAM_SINK="mysql"  # 可选: file, kafka, mysql
```

### 2. Delta Lake 格式配置

仅在 `feature/delta-lake` 分支可用：

```bash
# 数据格式选择
export DATA_FORMAT="delta"  # 默认: parquet, 可选: delta

# 或使用 JVM 参数
spark-submit --conf spark.DATA_FORMAT=delta ...
```

### 3. Windows 环境 Delta Lake 配置

Delta Lake 在 Windows 上需要 Hadoop Winutils：

**方法 1：设置 HADOOP_HOME**
```powershell
# 下载 winutils.exe 和 hadoop.dll
# 从 https://github.com/steveloughran/winutils 下载对应版本

# 设置环境变量
$env:HADOOP_HOME = "C:\hadoop"
# 将 winutils.exe 放在 C:\hadoop\bin\ 目录下
```

**方法 2：禁用事件日志**
如果不需要 Spark 事件日志，项目会自动检测并禁用（已实现）。

## 构建与运行

### 构建项目

```powershell
# feature/mysql-sink 分支
git checkout feature/mysql-sink
.\build-local.ps1

# feature/delta-lake 分支
git checkout feature/delta-lake
.\build-local.ps1
```

### 运行示例

```bash
# 运行所有批处理模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 运行 Module2 (SQL 分析 + MySQL sink)
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2

# 运行 Module3 (流式处理 + MySQL sink)
export STREAM_SINK=mysql
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 3

# 使用 Delta 格式运行（仅 feature/delta-lake 分支）
export DATA_FORMAT=delta
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all
```

## MySQL 表结构

需要预先创建以下表：

```sql
-- 漏斗报表
CREATE TABLE report_funnel (
    category VARCHAR(50),
    pv_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    conv_rate_pct DECIMAL(5,2)
);

-- 小时趋势
CREATE TABLE report_hour_trend (
    hour INT,
    pv BIGINT,
    uv BIGINT
);

-- Top3 商品
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
```

## 验证功能

### 验证 MySQL Sink

```sql
-- 查看批处理报表
SELECT * FROM report_funnel;
SELECT * FROM report_hour_trend;
SELECT * FROM report_top3_items;

-- 查看流式指标（需要运行 Module3）
SELECT * FROM stream_metrics ORDER BY window_start DESC LIMIT 10;
```

### 验证 Delta Lake

```bash
# 检查输出目录是否有 _delta_log 文件夹
ls output/sql_report/funnel_report/_delta_log
ls output/sql_report/hour_trend/_delta_log
ls output/sql_report/top3_items/_delta_log
```

## 故障排除

### MySQL 连接失败
- 检查 MySQL 服务是否运行
- 验证连接字符串、用户名、密码
- 确认数据库和表已创建
- 项目会捕获 MySQL 错误并继续执行（不会中断）

### Delta Lake 错误
- Windows: 确保 HADOOP_HOME 已设置或禁用事件日志
- 检查 Delta 依赖是否正确加载
- 验证 DATA_FORMAT 配置值（只能是 parquet 或 delta）

### 依赖冲突
```bash
# 清理并重新构建
.\build-local.ps1 -SkipTests
```

## 技术细节

### MySQL Sink 实现
- **批处理**：使用 DataFrame.write.jdbc() + SaveMode.Overwrite
- **流式**：使用 foreachBatch + SaveMode.Append
- **性能优化**：配置 batchsize 和 numPartitions

### Delta Lake 实现
- **依赖**：delta-spark_2.12:3.0.0（兼容 Spark 3.3.2）
- **配置**：自动注入 DeltaSparkSessionExtension 和 DeltaCatalog
- **格式切换**：通过 AppConfig.DATA_FORMAT 统一控制

## 参考资料

- [Delta Lake 官方文档](https://docs.delta.io/)
- [Spark Structured Streaming + JDBC](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Hadoop Winutils for Windows](https://github.com/steveloughran/winutils)
