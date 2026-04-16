# 分支使用指南

## 分支结构

```
test (基础版本)
  └── feature/no-datalake (无数据湖)
       └── feature/with-datalake (有数据湖)
            └── feature/with-datawarehouse (有数据仓库)
```

## 分支对比

| 特性 | no-datalake | with-datalake | with-datawarehouse |
|------|-------------|---------------|-------------------|
| **MySQL Sink** | ✅ | ✅ | ✅ |
| **漏斗分析** | ✅ | ✅ | ✅ |
| **Delta Lake** | ❌ | ✅ | ✅ |
| **数据仓库** | ❌ | ❌ | ✅ |
| **存储格式** | Parquet | Parquet/Delta | Parquet/Delta |
| **Module 7** | ❌ | ❌ | ✅ |

## 分支详情

### feature/no-datalake (无数据湖)

**包含功能：**
- ✅ MySQL Sink (Module2 批处理 + Module3 流式)
- ✅ 漏斗分析组件 (全局/分类/时段/用户分群/流失分析)
- ✅ ASCII 漏斗图 + ECharts 配置生成
- ✅ 6 个核心模块 (RDD/SQL/Streaming/GraphX/MLlib/Performance)

**存储格式：**
- 固定使用 Parquet 格式

**适用场景：**
- 不需要 ACID 事务
- 简单的数据分析需求
- 快速部署和测试

**切换命令：**
```bash
git checkout feature/no-datalake
```

---

### feature/with-datalake (有数据湖) ⭐ 推荐

**包含功能：**
- ✅ 所有 no-datalake 的功能
- ✅ Delta Lake 支持 (ACID 事务、时间旅行、Schema 演进)
- ✅ 配置驱动的格式切换 (Parquet/Delta)

**存储格式：**
- 通过 `DATA_FORMAT` 环境变量切换
- 默认 Parquet，可切换到 Delta

**新增依赖：**
- `delta-spark_2.12:3.0.0`

**新增配置：**
- `DATA_FORMAT=parquet|delta`
- SparkSession 自动配置 Delta 扩展

**适用场景：**
- 需要 ACID 事务保证
- 需要数据版本管理
- 生产环境数据湖

**切换命令：**
```bash
git checkout feature/with-datalake
```

**使用示例：**
```bash
# 使用 Parquet 格式
export DATA_FORMAT=parquet
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 使用 Delta 格式
export DATA_FORMAT=delta
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all
```

---

### feature/with-datawarehouse (有数据仓库) 🚀 最完整

**包含功能：**
- ✅ 所有 with-datalake 的功能
- ✅ 完整的数据仓库分层架构 (ODS/DWD/DWS/ADS)
- ✅ Module 7: 数据仓库构建

**数据仓库架构：**

#### ODS 层 (Operational Data Store)
- 原始数据层
- 直接加载原始日志
- 添加加载时间和数据日期

#### DWD 层 (Data Warehouse Detail)
- 明细数据层
- `user_behavior`: 清洗后的用户行为明细
- `user_profile`: 用户画像 (活跃度、转化率)
- `item_profile`: 商品画像 (热度、转化率)

#### DWS 层 (Data Warehouse Service)
- 汇总数据层
- `user_daily`: 用户日汇总
- `item_daily`: 商品日汇总
- `category_daily`: 类目日汇总

#### ADS 层 (Application Data Service)
- 应用数据层
- `user_metrics`: 用户指标 (价值评分、活跃度)
- `item_metrics`: 商品指标 (热度评分、销量)
- `category_metrics`: 类目指标 (市场份额、排名)

**输出目录：**
```
output/warehouse/
├── ods/
│   └── user_behavior/
├── dwd/
│   ├── user_behavior/
│   ├── user_profile/
│   └── item_profile/
├── dws/
│   ├── user_daily/
│   ├── item_daily/
│   └── category_daily/
└── ads/
    ├── user_metrics/
    ├── item_metrics/
    └── category_metrics/
```

**适用场景：**
- 完整的数据分析平台
- 需要多层次数据建模
- 支持复杂的业务分析

**切换命令：**
```bash
git checkout feature/with-datawarehouse
```

**运行数据仓库：**
```bash
# 运行 Module 7 构建数据仓库
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 7

# 或运行所有模块（包括数据仓库）
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all
```

## 核心差异对比

### 1. 依赖差异

**no-datalake:**
```xml
<!-- 无 Delta Lake 依赖 -->
```

**with-datalake / with-datawarehouse:**
```xml
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-spark_2.12</artifactId>
    <version>3.0.0</version>
</dependency>
```

### 2. 配置差异

**no-datalake:**
```scala
// 无 DATA_FORMAT 配置
// 固定使用 parquet
```

**with-datalake / with-datawarehouse:**
```scala
// 支持格式切换
val DATA_FORMAT: String = cfgString("DATA_FORMAT", "parquet").toLowerCase
```

### 3. SparkSession 差异

**no-datalake:**
```scala
// 无 Delta 扩展
val conf = new SparkConf()
  .setAppName(AppConfig.APP_NAME)
  // ... 其他配置
```

**with-datalake / with-datawarehouse:**
```scala
val conf = new SparkConf()
  .setAppName(AppConfig.APP_NAME)
  .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  // ... 其他配置
```

### 4. 模块差异

**no-datalake / with-datalake:**
- Module 1-6

**with-datawarehouse:**
- Module 1-7 (新增 Module 7: 数据仓库构建)

## 快速选择指南

### 选择 no-datalake 如果：
- ✅ 只需要基础的数据分析
- ✅ 不需要 ACID 事务
- ✅ 快速原型开发
- ✅ 学习 Spark 基础功能

### 选择 with-datalake 如果：
- ✅ 需要数据一致性保证
- ✅ 需要版本管理和时间旅行
- ✅ 生产环境部署
- ✅ 现代数据湖架构

### 选择 with-datawarehouse 如果：
- ✅ 需要完整的数据仓库
- ✅ 多层次数据建模
- ✅ 复杂的业务分析
- ✅ 企业级数据平台

## 构建和运行

### 构建项目

```bash
# 切换到目标分支
git checkout feature/with-datawarehouse

# 构建
cd ecommerce-spark
.\.build-env\apache-maven-3.9.9\bin\mvn.cmd clean package -DskipTests
```

### 运行示例

```bash
# 运行所有模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 运行单个模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2  # SQL 分析
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 7  # 数据仓库

# 使用 Delta 格式
export DATA_FORMAT=delta
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all
```

## 分支维护

### 更新 no-datalake 分支
```bash
git checkout feature/no-datalake
# 进行修改
git add -A
git commit -m "your message"
```

### 同步到 with-datalake 分支
```bash
git checkout feature/with-datalake
git merge feature/no-datalake
# 解决冲突（如果有）
git commit
```

### 同步到 with-datawarehouse 分支
```bash
git checkout feature/with-datawarehouse
git merge feature/with-datalake
# 解决冲突（如果有）
git commit
```

## 总结

- **no-datalake**: 基础版，适合学习和快速开发
- **with-datalake**: 生产版，适合实际部署
- **with-datawarehouse**: 完整版，适合企业级应用

根据你的需求选择合适的分支！
