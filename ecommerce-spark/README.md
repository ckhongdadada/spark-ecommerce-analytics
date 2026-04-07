# 电商全路径数据分析系统

基于 Scala 2.12 和 Apache Spark 3.3.2 的课程项目，覆盖 RDD、Spark SQL、Structured Streaming、GraphX、MLlib 和性能调优六个模块。

## 当前状态

- 已统一 Spark SQL 时区为 `Asia/Shanghai`
- `all` 模式默认只运行批处理模块，不包含需要常驻的 Streaming
- Streaming 的 checkpoint 已放到 `writeStream`
- Module1 重复运行时会自动清理旧输出目录
- Module4 改为 `zipWithUniqueId` 分配顶点 ID，避免哈希碰撞
- 已补充本地隔离构建脚本和基础单元测试

## 目录说明

```text
ecommerce-spark/
├── build-local.ps1
├── pom.xml
├── src/main/scala/com/ecommerce/
│   ├── Main.scala
│   ├── config/AppConfig.scala
│   ├── core/
│   ├── module/
│   └── util/
├── src/main/resources/logback.xml
└── src/test/scala/com/ecommerce/
```

## 本地隔离构建

项目使用根目录下的 `.build-env` 作为本地构建环境。

```powershell
# 跳过测试
.\build-local.ps1 -SkipTests

# 完整构建
.\build-local.ps1
```

如果你不使用脚本，也可以直接调用本地 Maven：

```powershell
.\.build-env\apache-maven-3.9.9\bin\mvn.cmd `
  -Dmaven.repo.local=.\.build-env\m2-repo `
  clean package
```

## 运行方式

```bash
# 默认运行全部批处理模块（1,2,4,5,6）
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 仅生成模拟数据
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar gen

# 单模块运行
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 1
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 3
```

## 模块说明

1. `Module1_DataPreprocessing`：RDD 清洗、行为统计、活跃用户分析
2. `Module2_SparkSQL`：漏斗报表、小时级 PV/UV、Top3 热销商品
3. `Module3_Streaming`：Kafka/Socket 双入口实时统计
4. `Module4_GraphX`：用户共购图、PageRank、社区发现
5. `Module5_MLlib`：购买行为二分类预测
6. `Module6_PerformanceTuning`：缓存、广播、分区、倾斜处理、执行计划分析

## 说明

- `Module3_Streaming` 中 UV 使用的是近似去重聚合，以保证流式查询可运行。
- `Module6_PerformanceTuning` 主要用于课程演示和对比观察，不应直接视为严格基准测试报告。
- 模拟数据由 `DataGenerator` 生成，适合演示，不等价于真实业务数据。
