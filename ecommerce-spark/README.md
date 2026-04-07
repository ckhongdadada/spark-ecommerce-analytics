# 电商全路径数据分析系统

基于 Scala 2.12 和 Apache Spark 3.3.2 的课程项目，覆盖 RDD、Spark SQL、Structured Streaming、GraphX、MLlib 和性能调优六个模块。

## 当前状态

- `all` 模式默认只运行批处理模块，不包含需要常驻的 Streaming。
- Spark SQL 统一使用 `Asia/Shanghai` 时区。
- Streaming 的 checkpoint 已放到 `writeStream`，窗口聚合结果落地为 Parquet 文件。
- 模块 1 支持重复运行，会自动清理旧输出目录。
- GraphX 顶点 ID 已改为 `zipWithUniqueId`，避免哈希碰撞风险。
- 模拟数据改为“用户画像 + 会话链路”，更接近真实电商行为路径。
- 模块 6 会输出结构化 benchmark 结果，并在环境支持时自动开启 Spark event log。
- 当前包含 10 个基础测试用例。

## 目录说明

```text
ecommerce-spark/
├── .build-env/
├── build-local.ps1
├── pom.xml
├── src/main/scala/com/ecommerce/
│   ├── Main.scala
│   ├── config/
│   ├── core/
│   ├── module/
│   └── util/
├── src/main/resources/logback.xml
└── src/test/scala/com/ecommerce/
```

## 本地隔离构建

项目使用根目录下的 `.build-env` 作为本地 Maven 构建环境。

```powershell
# 跳过测试
.\build-local.ps1 -SkipTests

# 完整构建
.\build-local.ps1
```

如果不使用脚本，也可以直接调用本地 Maven：

```powershell
.\.build-env\apache-maven-3.9.9\bin\mvn.cmd `
  -Dmaven.repo.local=.\.build-env\m2-repo `
  clean package
```

## 运行方式

```bash
# 运行全部批处理模块（1,2,4,5,6）
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 仅生成模拟数据
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar gen

# 单模块运行
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 1
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 3
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 6
```

## 模块说明

1. `Module1_DataPreprocessing`：RDD 清洗、行为统计、活跃用户分析。
2. `Module2_SparkSQL`：转化漏斗、小时级 PV/UV、类目热销商品排行。
3. `Module3_Streaming`：Kafka/Socket 双入口实时统计，输出落地到 `output/streaming/`。
4. `Module4_GraphX`：用户共购图、PageRank、社区发现。
5. `Module5_MLlib`：购买行为二分类预测。
6. `Module6_PerformanceTuning`：缓存、广播、分区、数据倾斜、结构化 benchmark 输出和执行计划分析。

## Benchmark 输出

运行模块 6 后，会在 `output/benchmark/` 生成以下结果：

- `measurements.csv`：每一次 warmup / measured 运行的明细耗时记录。
- `summaries.csv`：每个实验场景的聚合统计结果。
- `explain_plan.txt`：当前执行计划文本，便于报告引用。
- `README.txt`：benchmark 结果说明。

如果运行环境支持本地 Hadoop 权限设置，Spark event log 会写入 `output/eventlog/`，可结合 Spark UI 或 History Server 分析 shuffle、spill 和 task skew。

## 说明

- `Module3_Streaming` 中 UV 使用近似去重聚合，以保证流式窗口查询可运行。
- Streaming 输出采用 Parquet 文件 sink，便于重复运行后直接查看落地结果。
- `Module6_PerformanceTuning` 现在适合用于课程答辩中的“可重复 benchmark 演示”，但仍不是严格意义上的工业级压测框架。
- 模拟数据由 `DataGenerator` 基于用户画像和会话链路生成，仍然是演示数据，但比纯随机采样更接近真实行为序列。
