import os
import great_expectations as gx
from spark_utils import get_spark_session
from quality_checks import QUALITY_CHECKS

def main():
    spark = get_spark_session("quality-checks")
    context = gx.get_context()
    data_source = context.data_sources.add_spark(name="silver_quality_source")

    total_checks = 0
    total_failed = 0

    for entity, config in QUALITY_CHECKS.items():
        print(f"--- {entity} ---")
        df = spark.table(config["table"])
        if "pre_filter" in config:
            df = df.filter(config["pre_filter"])
        asset = data_source.add_dataframe_asset(name=f"{entity}_asset")
        batch_def = asset.add_batch_definition_whole_dataframe(f"{entity}_batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        for check in config["checks"]:
            result = batch.validate(check)
            total_checks += 1
            status = "PASS" if result.success else "FAIL"
            if not result.success:
                total_failed += 1
                unexpected = result.result.get("unexpected_count", "?")
                print(f"  [{status}] {check.column}: {unexpected} unexpected values")
            else:
                print(f"  [{status}] {check.column}")

    print(f"\nTotal checks: {total_checks}, Failed: {total_failed}, Passed: {total_checks - total_failed}")
    spark.stop()
    os._exit(0)

if __name__ == "__main__":
    main()
