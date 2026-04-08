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

## 当前改造基线（已完成）

以下改动已经落地，可作为你后续改题时的“基础模板”：

- 项目名称、业务术语、行为标签、类目、模块开关、`all` 模式执行清单统一收口到 `AppConfig.scala`。
- 入口改为“元数据驱动调度”，`Main.scala` 按配置决定模块名称、是否启用、`all` 跑哪些模块。
- 核心行为语义不再散落硬编码，模型统一读取配置（`Models.scala`）。
- 模块 1~6 已接入统一语义配置（行为值、类目、模块标题等）：
  - `Module1_DataPreprocessing.scala`
  - `Module2_SparkSQL.scala`
  - `Module3_Streaming.scala`
  - `Module4_GraphX.scala`
  - `Module5_MLlib.scala`
  - `Module6_PerformanceTuning.scala`
- 数据生成器改为读取配置语义（`DataGenerator.scala`）。
- README 已补充适配说明，并新增配置一致性测试（`AppConfigAndModelTest.scala`）。

## 验证结果（最新）

- 全量构建：`BUILD SUCCESS`
- 单元测试：`12` 个全部通过

## 具体要求出来后，优先改这些部分

先看要求属于哪一类，再改对应文件：

1. 只改“题目名称/业务术语/行为定义/类目命名”
- 必改：`src/main/scala/com/ecommerce/config/AppConfig.scala`
- 同步文档：`README.md`、课程报告文档
- 通常不用改模块实现代码

2. 调整“执行范围”（例如 `all` 模式不跑某些模块，或模块重命名）
- 必改：`src/main/scala/com/ecommerce/config/AppConfig.scala`
- 可能改：`src/main/scala/com/ecommerce/Main.scala`（若入口规则被老师额外限制）

3. 调整“指标口径/统计逻辑”
- 必改：对应模块文件（例如 SQL 指标改动就改 `Module2_SparkSQL.scala`）
- 建议同步：更新该模块输出说明和 README 示例

4. 调整“数据结构/字段含义/样本分布”
- 必改：`src/main/scala/com/ecommerce/util/DataGenerator.scala`
- 联动检查：`Models.scala` 与各模块字段映射

5. 调整“实时链路”（sink、容错、吞吐约束）
- 必改：`Module3_Streaming.scala`
- 联动：`AppConfig.scala` 的流式配置项（输出路径、checkpoint、Kafka 地址等）

6. 调整“模型方案/评估标准”
- 必改：`Module5_MLlib.scala`
- 建议补测：新增或更新与模型评估相关测试用例

7. 调整“性能实验要求/benchmark 交付物”
- 必改：`Module6_PerformanceTuning.scala`
- 同步检查：`output/benchmark/` 输出格式是否满足老师要求

如果不确定该改哪一块，优先从 `AppConfig.scala` 入手，再定位到单个模块细改，避免全项目重写。

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
