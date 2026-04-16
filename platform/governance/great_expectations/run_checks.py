"""
Great Expectations 数据质量校验
用于 Airflow DAG 调度
"""
import sys
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import datetime


def run_quality_checks(dt=None):
    if dt is None:
        dt = datetime.now().strftime("%Y-%m-%d")

    print(f"Running data quality checks for dt={dt}")

    context = gx.get_context()

    suite = context.add_expectation_suite("ecommerce_quality_suite")

    datasource = context.get_datasource("clickhouse_source")

    batch_request = RuntimeBatchRequest(
        datasource_name="clickhouse_source",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="ads_category_metrics",
        batch_identifiers={"default_identifier_name": dt},
        runtime_parameters={
            "query": f"SELECT * FROM ads_category_metrics FINAL"
        },
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="ecommerce_quality_suite",
    )

    validator.expect_column_to_exist("category")
    validator.expect_column_to_exist("total_buys")
    validator.expect_column_to_exist("market_share")
    validator.expect_column_values_to_not_be_null("category")
    validator.expect_column_values_to_be_between("market_share", min_value=0, max_value=100)
    validator.expect_column_values_to_be_between("avg_conversion_rate", min_value=0, max_value=100)
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000)

    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(
        name="realtime_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": "ecommerce_quality_suite",
            }
        ],
    )

    result = checkpoint.run()

    if result["success"]:
        print("Data quality checks PASSED")
    else:
        print("Data quality checks FAILED")
        for validation_result in result.run_results.values():
            for sr in validation_result["validation_result"].results:
                if not sr.success:
                    print(f"  FAILED: {sr.expectation_config.expectation_type} - {sr.expectation_config.kwargs}")

    return result["success"]


if __name__ == "__main__":
    dt = sys.argv[1] if len(sys.argv) > 1 else None
    success = run_quality_checks(dt)
    sys.exit(0 if success else 1)
