# Ecommerce Spark Analytics (Template-Friendly)

This is a Scala + Spark course project that includes:
- RDD preprocessing
- Spark SQL reporting
- Structured Streaming
- GraphX
- MLlib
- Performance benchmark module

## Why it is easier to adapt now

The project is now more metadata-driven.  
If assignment wording changes later, you usually only need to update:

- `src/main/scala/com/ecommerce/config/AppConfig.scala`
- `src/main/scala/com/ecommerce/Main.scala`
- `README.md` and report text

Key configurable areas in `AppConfig`:
- project display name/version/domain label
- behavior labels (`pv/buy/cart/fav`)
- category list and category-to-item generator settings
- module catalog (name, enabled switch, include-in-all switch)
- run paths and benchmark paths

## Build

Use the local isolated Maven environment:

```powershell
# full build
.\build-local.ps1

# build without tests
.\build-local.ps1 -SkipTests
```

## Run

```bash
# run all enabled batch modules configured in AppConfig.ALL_MODE_MODULE_IDS
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar all

# generate demo data only
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar gen

# run one module
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 1
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 3
spark-submit --class com.ecommerce.Main target/ecommerce-spark-analysis-1.0.0.jar 6
```

## Benchmark output

After running module `6`, outputs are written to `output/benchmark/`:
- `measurements.csv`
- `summaries.csv`
- `explain_plan.txt`
- `README.txt`

If runtime environment supports it, Spark event logs are written to `output/eventlog/`.

## Practical adaptation guide

If your final assignment asks for a different business theme:

1. Update labels and semantics in `AppConfig` (domain name, behavior labels, category list).
2. Adjust enabled modules and display names in `AppConfig.MODULE_DEFINITIONS`.
3. Keep module code structure unchanged unless required by new metrics.
4. Re-run `.\build-local.ps1` to validate all tests and packaging.
