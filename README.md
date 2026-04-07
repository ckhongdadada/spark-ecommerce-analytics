# Spark 项目说明

这个目录包含 Spark 课程项目的代码、说明文档和论文稿：

- `ecommerce-spark/`：当前可构建、可运行的项目源码
- `spark基本版本.docx`：基础版论文/说明
- `spark改进版本.docx`：改进版论文/说明
- `spark模本.docx`：文档模板

## 建议阅读顺序

1. 先看 `ecommerce-spark/README.md`
2. 再看 `ecommerce-spark/项目审视报告.md`
3. 最后对照 `spark改进版本.docx`

## 当前推荐构建方式

进入 `ecommerce-spark` 后执行：

```powershell
.\build-local.ps1
```

这会使用项目内的 `.build-env` 本地构建环境，不依赖系统级 Maven。