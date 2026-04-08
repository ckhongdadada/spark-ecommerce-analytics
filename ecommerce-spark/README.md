# 电商 Spark 分析项目（模板友好版）

本项目基于 Scala + Spark，包含以下核心模块：
- RDD 数据预处理
- Spark SQL 报表分析
- Structured Streaming 实时统计
- GraphX 图分析
- MLlib 行为预测
- 性能调优与 Benchmark

## 为什么现在更容易适配期末要求

当前代码已经改为“配置驱动”结构。  
后续作业题目变更时，通常只需要优先修改：

- `src/main/scala/com/ecommerce/config/AppConfig.scala`
- `src/main/scala/com/ecommerce/Main.scala`
- `README.md` 与报告文案

`AppConfig` 中已经集中管理：
- 项目名称、版本、领域标签
- 行为标签（`pv/buy/cart/fav`）
- 类目集合与数据生成规则
- 模块目录（名称、启用开关、`all` 模式是否纳入）
- 各模块输入输出路径与 benchmark 路径

## 构建方式

使用项目内隔离 Maven 环境：

```powershell
# 完整构建
.\build-local.ps1

# 跳过测试
.\build-local.ps1 -SkipTests
```

## 运行方式

```bash
# 运行 AppConfig.ALL_MODE_MODULE_IDS 配置的全部批处理模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# 仅生成模拟数据
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar gen

# 运行单模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 1
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 3
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 6
```

## Benchmark 输出

运行模块 `6` 后，会在 `output/benchmark/` 生成：
- `measurements.csv`
- `summaries.csv`
- `explain_plan.txt`
- `README.txt`

如果运行环境支持，会同时写入 `output/eventlog/` 作为 Spark 事件日志。

## 期末作业快速适配建议

如果最终作业要求换业务主题：

1. 在 `AppConfig` 中先改领域标签、行为语义、类目定义。
2. 在 `AppConfig.MODULE_DEFINITIONS` 中改模块名称与启用策略。
3. 仅在指标逻辑真的变化时再调整对应模块代码。
4. 最后运行 `.\build-local.ps1` 做完整验证。
