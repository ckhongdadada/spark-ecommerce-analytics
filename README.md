# Spark 课程项目说明

本仓库用于 Spark 课程大作业，主要包含两部分：

- `ecommerce-spark/`：可直接构建运行的项目源码
- 论文/报告资料：`spark基本版本.docx`、`spark改进版本.docx`、`spark模本.docx`

## 建议阅读顺序

1. 先阅读 `ecommerce-spark/README.md`
2. 再查看 `ecommerce-spark/项目审视报告.md`
3. 最后对照论文文档完善答辩材料

## 推荐构建方式

进入 `ecommerce-spark` 目录后执行：

```powershell
.\build-local.ps1
```

该方式会使用项目内隔离环境（`.build-env`），不依赖系统级 Maven 配置。
