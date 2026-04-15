# 用户行为漏斗分析组件使用指南

## 概述

用户行为漏斗分析组件实现了完整的"曝光 -> 点击 -> 加购 -> 支付"四步转化率分析，提供多维度的漏斗分析和可视化支持。

## 核心功能

### 1. 多维度漏斗分析

#### 全局漏斗 (Global Funnel)
- 分析整体用户的四步转化情况
- 计算每步转化率和流失率
- 识别整体转化瓶颈

#### 分类漏斗 (Category Funnel)
- 按商品类目分析转化情况
- 对比不同类目的转化效率
- 识别高/低转化类目

#### 时段漏斗 (Hourly Funnel)
- 按小时分析转化情况
- 识别高峰/低谷时段
- 优化运营时间策略

#### 用户分群漏斗 (User Segment Funnel)
- 按用户活跃度分群分析
- 对比不同用户群的转化特征
- 制定差异化运营策略

### 2. 流失分析 (Loss Analysis)
- 识别每个环节的流失用户数
- 计算流失率
- 定位最大流失环节

### 3. 可视化支持

#### ASCII 艺术漏斗图
- 命令行直接展示
- 直观的漏斗形状
- 实时查看转化情况

#### ECharts 配置生成
- 生成前端可用的 JSON 配置
- 支持交互式漏斗图
- 适配主流可视化框架

## 使用方式

### 运行漏斗分析

漏斗分析已集成到 Module2 中，运行 Module2 即可自动执行：

```bash
# 运行 Module2 (包含漏斗分析)
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 2

# 或运行所有模块
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all
```

### 输出结果

#### 文件输出

所有漏斗分析结果保存在 `output/sql_report/funnel_analysis/` 目录：

```
output/sql_report/funnel_analysis/
├── funnel_global/              # 全局漏斗数据
├── funnel_category/            # 分类漏斗数据
├── funnel_hourly/              # 时段漏斗数据
├── funnel_user_segment/        # 用户分群漏斗数据
├── funnel_loss_analysis/       # 流失分析数据
└── visualization/              # 可视化配置
    ├── funnel_ascii.txt        # ASCII 艺术漏斗图
    ├── funnel_echarts_global.json
    ├── funnel_echarts_Electronics.json
    ├── funnel_echarts_Clothing.json
    └── ...
```

#### MySQL 输出

如果配置了 MySQL，漏斗分析结果会写入以下表：

- `funnel_global` - 全局漏斗
- `funnel_category` - 分类漏斗
- `funnel_hourly` - 时段漏斗
- `funnel_user_segment` - 用户分群漏斗
- `funnel_loss_analysis` - 流失分析

#### 控制台输出

运行时会在控制台打印：
1. ASCII 艺术漏斗图
2. 各维度漏斗数据表格
3. 流失分析报告
4. 类目转化率对比

## 数据结构

### 漏斗分析表结构

```sql
CREATE TABLE funnel_global (
    dimension VARCHAR(50),                    -- 维度名称
    step1_exposure_users BIGINT,              -- 曝光用户数
    step2_click_users BIGINT,                 -- 点击用户数
    step3_cart_users BIGINT,                  -- 加购用户数
    step4_buy_users BIGINT,                   -- 支付用户数
    step1_exposure_events BIGINT,             -- 曝光事件数
    step2_click_events BIGINT,                -- 点击事件数
    step3_cart_events BIGINT,                 -- 加购事件数
    step4_buy_events BIGINT,                  -- 支付事件数
    step1_to_step2_rate DECIMAL(5,2),         -- 曝光到点击转化率(%)
    step2_to_step3_rate DECIMAL(5,2),         -- 点击到加购转化率(%)
    step3_to_step4_rate DECIMAL(5,2),         -- 加购到支付转化率(%)
    overall_conversion_rate DECIMAL(5,2),     -- 整体转化率(%)
    step1_to_step2_loss_rate DECIMAL(5,2),    -- 曝光到点击流失率(%)
    step2_to_step3_loss_rate DECIMAL(5,2),    -- 点击到加购流失率(%)
    step3_to_step4_loss_rate DECIMAL(5,2)     -- 加购到支付流失率(%)
);

-- 其他漏斗表结构相同，只是 dimension 字段含义不同
```

### 流失分析表结构

```sql
CREATE TABLE funnel_loss_analysis (
    loss_stage VARCHAR(100),      -- 流失阶段
    loss_users BIGINT,            -- 流失用户数
    loss_rate DECIMAL(5,2)        -- 流失率(%)
);
```

## 业务价值

### 1. 转化优化
- **识别瓶颈**：快速定位转化率最低的环节
- **量化流失**：精确计算每个环节的流失用户数
- **对比分析**：横向对比不同维度的转化效率

### 2. 精细化运营
- **类目策略**：针对低转化类目制定专项优化方案
- **时段优化**：在高转化时段加大推广力度
- **用户分层**：对不同活跃度用户采取差异化策略

### 3. 商业洞察
- **用户行为理解**：深入理解用户购买决策路径
- **产品优化方向**：基于流失环节优化产品体验
- **营销效果评估**：量化营销活动对转化的影响

## 可视化示例

### ASCII 漏斗图示例

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

### ECharts 配置使用

生成的 JSON 配置可直接用于 ECharts：

```javascript
// 读取生成的配置文件
fetch('output/sql_report/funnel_analysis/visualization/funnel_echarts_global.json')
  .then(response => response.json())
  .then(option => {
    const chart = echarts.init(document.getElementById('funnel-chart'));
    chart.setOption(option);
  });
```

## 高级用法

### 自定义漏斗分析

如果需要自定义漏斗分析逻辑，可以直接使用 `FunnelAnalysis` 对象：

```scala
import com.ecommerce.analytics.FunnelAnalysis
import com.ecommerce.core.SparkSessionFactory

val spark = SparkSessionFactory.getSession()
val rawDF = spark.read.parquet("path/to/data")

// 构建全局漏斗
val globalFunnel = FunnelAnalysis.buildGlobalFunnel(rawDF)
globalFunnel.show()

// 构建分类漏斗
val categoryFunnel = FunnelAnalysis.buildCategoryFunnel(rawDF)
categoryFunnel.show()

// 生成可视化
import com.ecommerce.analytics.FunnelVisualization
val asciiArt = FunnelVisualization.generateASCIIFunnel(globalFunnel, "Global")
println(asciiArt)
```

### 自定义用户分群

可以修改 `buildUserSegmentFunnel` 方法中的分群逻辑：

```scala
// 当前分群规则：
// - 高活跃用户: >= 50 次行为
// - 中活跃用户: >= 20 次行为
// - 低活跃用户: >= 5 次行为
// - 新用户: < 5 次行为

// 可根据业务需求调整阈值
```

## 性能优化

### 数据量大时的优化建议

1. **采样分析**：对大数据集先进行采样
```scala
val sampledDF = rawDF.sample(withReplacement = false, 0.1)
val funnel = FunnelAnalysis.buildGlobalFunnel(sampledDF)
```

2. **缓存中间结果**：
```scala
rawDF.cache()
val globalFunnel = FunnelAnalysis.buildGlobalFunnel(rawDF)
val categoryFunnel = FunnelAnalysis.buildCategoryFunnel(rawDF)
rawDF.unpersist()
```

3. **分区优化**：
```scala
val repartitionedDF = rawDF.repartition(col("category"))
```

## 常见问题

### Q1: 为什么曝光用户数和点击用户数相同？
A: 当前实现中，曝光和点击都使用 `pv` 行为。如果需要区分，可以在数据生成时添加独立的曝光事件。

### Q2: 如何解读流失率？
A: 流失率 = (上一步用户数 - 当前步用户数) / 上一步用户数 × 100%。流失率越高，说明该环节转化障碍越大。

### Q3: 用户分群的阈值如何确定？
A: 建议先运行一次分析，查看用户行为次数分布，然后根据业务特点设置合理的分群阈值。

### Q4: 可以添加更多维度的漏斗分析吗？
A: 可以。参考现有的 `buildCategoryFunnel` 方法，按照相同模式添加新的维度分析。

## 扩展建议

### 1. 时间窗口漏斗
分析用户在特定时间窗口内（如 30 分钟）完成转化的情况。

### 2. 路径分析
分析用户从曝光到支付的完整路径，识别高效路径和低效路径。

### 3. 归因分析
分析不同营销渠道、活动对转化的贡献度。

### 4. 预测模型
基于漏斗数据训练模型，预测用户在各环节的转化概率。

## 参考资料

- [ECharts 漏斗图文档](https://echarts.apache.org/zh/option.html#series-funnel)
- [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [用户行为分析最佳实践](https://www.example.com/user-behavior-analysis)
