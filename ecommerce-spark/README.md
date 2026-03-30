# 电商全路径数据分析系统
## Spark 期末大作业 · 完整操作手册

---

## 一、提纲审查报告

### ✅ 原提纲已覆盖的课程内容
| 课程章节 | 提纲模块 | 状态 |
|---|---|---|
| 第2章·RDD编程/键值对/数据读写 | 模块一：数据预处理 | ✅ 已覆盖 |
| 第3章·Spark SQL | 模块二：业务报表 | ✅ 已覆盖 |
| 第3章·Spark Streaming | 模块三：实时监控 | ✅ 已覆盖 |
| 第3章·GraphX | 模块四：社交挖掘 | ✅ 已覆盖 |
| 第3章·MLlib | 模块五：行为预测 | ✅ 已覆盖 |

### ❌ 原提纲的不足
| 问题 | 说明 |
|---|---|
| **遗漏第4章** | 课程目录明确列出「第4章 Spark性能调优」，原提纲完全未涉及 |
| **第2章知识点不完整** | 「Spark集群安装与部署」「数据读取与保存」未体现在提纲中 |
| **交付物缺乏Web UI** | 课程实验要求提交 Spark Web UI 截图，原提纲提到但无具体说明 |
| **Streaming降级方案** | 无本地调试方案，本地环境无 Kafka 时作业无法运行 |

### ✅ 完善后新增内容
- **模块六：性能调优**（对应第4章），覆盖 Cache/广播变量/分区优化/Skew Join/执行计划分析
- 数据生成器（无需真实数据集即可本地运行）
- Streaming Socket 降级方案（无 Kafka 也能演示）
- 完整 Web UI 截图指南

---

## 二、项目结构

```
ecommerce-spark/
├── pom.xml                          # Maven 依赖管理
├── data/mock/
│   └── user_behavior_log.csv        # 自动生成的模拟数据（首次运行自动创建）
├── output/                          # 各模块输出目录（运行后生成）
│   ├── cleaned/                     # 模块一：清洗后数据
│   ├── sql_report/                  # 模块二：Parquet 报表
│   └── model/                       # 模块五：MLlib 模型
└── src/main/scala/com/ecommerce/
    ├── Main.scala                   # 主入口（支持按模块运行）
    ├── config/
    │   └── AppConfig.scala          # 全局配置（路径/参数统一管理）
    ├── core/
    │   ├── SparkSessionFactory.scala # Spark 单例工厂（含性能调优配置）
    │   └── Models.scala             # 公共数据模型（case class）
    ├── module/
    │   ├── Module1_DataPreprocessing.scala  # RDD 数据预处理
    │   ├── Module2_SparkSQL.scala           # Spark SQL 报表
    │   ├── Module3_Streaming.scala          # Structured Streaming
    │   ├── Module4_GraphX.scala             # GraphX 社交挖掘
    │   ├── Module5_MLlib.scala              # MLlib 行为预测
    │   └── Module6_PerformanceTuning.scala  # ★ 性能调优（第4章）
    └── util/
        └── DataGenerator.scala      # 模拟数据生成器
```

---

## 三、详细操作步骤

### 第一步：环境准备

```bash
# 1. 确认 Java 版本（必须 >= 1.8）
java -version

# 2. 确认 Scala 版本（推荐 2.12.x）
scala -version

# 3. 确认 Maven 版本（>= 3.6）
mvn -version

# 4. 确认 Spark 安装（本地模式可跳过，YARN 需要）
spark-submit --version
```

### 第二步：克隆/解压项目并编译

```bash
# 进入项目目录
cd ecommerce-spark

# 编译 + 打包（首次约3-5分钟下载依赖）
mvn clean package -DskipTests

# 成功后在 target/ 目录下生成：
# ecommerce-spark-analysis-1.0.0.jar
```

### 第三步：生成模拟数据（可选，首次运行自动触发）

```bash
spark-submit \
  --class com.ecommerce.util.DataGenerator \
  --master local[*] \
  target/ecommerce-spark-analysis-1.0.0.jar \
  data/mock/user_behavior_log.csv 100000
```

生成的 CSV 格式：
```
userId,itemId,category,behavior,timestamp
U1234,I5678,Electronics,pv,1700001234
U2345,I6789,Clothing,buy,1700002345
...
```

### 第四步：运行各模块

#### 本地模式（开发调试）
```bash
# 运行全部模块
spark-submit \
  --class com.ecommerce.Main \
  --master local[*] \
  --driver-memory 2g \
  target/ecommerce-spark-analysis-1.0.0.jar all

# 单独运行模块一（RDD）
spark-submit --class com.ecommerce.Main --master local[*] \
  target/ecommerce-spark-analysis-1.0.0.jar 1

# 单独运行模块六（性能调优）
spark-submit --class com.ecommerce.Main --master local[*] \
  target/ecommerce-spark-analysis-1.0.0.jar 6
```

#### Spark on YARN（集群模式）
```bash
# 上传数据到 HDFS
hdfs dfs -mkdir -p /ecommerce/data
hdfs dfs -put data/mock/user_behavior_log.csv /ecommerce/data/

# 修改 AppConfig.scala 中的路径为 HDFS 路径
# RAW_LOG_PATH = "hdfs:///ecommerce/data/user_behavior_log.csv"
# 重新编译后提交：

spark-submit \
  --class com.ecommerce.Main \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  target/ecommerce-spark-analysis-1.0.0.jar all
```

#### 模块三·Streaming 调试（Socket 模式）
```bash
# 终端1：启动数据发送端
nc -lk 9999

# 终端2：启动 Streaming 任务
spark-submit \
  -Dstreaming.source=socket \
  --class com.ecommerce.Main \
  --master local[2] \
  target/ecommerce-spark-analysis-1.0.0.jar 3

# 在终端1输入模拟数据（回车发送）：
# U1,I1,Electronics,pv,1700000001
# U2,I2,Clothing,buy,1700000002
```

### 第五步：查看 Spark Web UI（截图用于报告）

```
本地模式：http://localhost:4040
YARN 模式：http://<ResourceManager>:8088
```

**需要截图的页面：**
1. Jobs 页面：各模块 Job 执行时间
2. Stages 页面：Task 并行度分布
3. Storage 页面：模块六 cache 后的持久化情况
4. SQL 页面：模块二 DataFrame 执行计划 DAG

### 第六步：验证输出结果

```bash
# 检查清洗数据（模块一）
ls -la output/cleaned/

# 检查 SQL 报表（模块二）
ls -la output/sql_report/funnel_report/

# 检查 MLlib 模型（模块五）
ls -la output/model/
```

---

## 四、各模块知识点对照表（期末大作业核查用）

| 模块 | 对应章节 | 核心知识点 | 代码位置 |
|---|---|---|---|
| 模块一 | 第2章 | filter/map/flatMap/PairRDD/reduceByKey/saveAsTextFile | Module1_DataPreprocessing.scala |
| 模块二 | 第3章 | SparkSession/DataFrame/临时视图/窗口函数/JDBC写出 | Module2_SparkSQL.scala |
| 模块三 | 第3章 | Structured Streaming/窗口统计/watermark/Socket/Kafka | Module3_Streaming.scala |
| 模块四 | 第3章 | Graph/Vertex/Edge/PageRank/ConnectedComponents/outDegrees | Module4_GraphX.scala |
| 模块五 | 第3章 | VectorAssembler/Pipeline/逻辑回归/随机森林/AUC/准确率 | Module5_MLlib.scala |
| **模块六** | **第4章** | **cache/persist/广播变量/repartition/Skew-Salt/explain** | **Module6_PerformanceTuning.scala** |

---

## 五、常见问题

**Q: 内存不足 OOM？**
A: 修改 AppConfig 中的 `EXECUTOR_MEMORY` 和 `DRIVER_MEMORY`，或减少 `rowCount`。

**Q: MySQL 连不上？**
A: 模块二中 MySQL 写出在 try-catch 内，连不上会自动跳过并提示，不影响其他功能。

**Q: Streaming 无输出？**
A: 确认 nc -lk 9999 已在另一终端运行，并在该终端输入数据后回车。

**Q: GraphX 运行很慢？**
A: 本地模式建议数据量 ≤ 50000 条，修改 DataGenerator 的 rowCount 参数。
