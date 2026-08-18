import os
from functools import reduce
from pyspark.sql.functions import col
from spark_utils import get_spark_session
from silver_config import SILVER_CONFIG

TECHNICAL_COLS = {"kafka_partition", "kafka_offset", "kafka_timestamp", "ingested_at"}

def main():
    spark = get_spark_session("silver-transform")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.quarantine")

    for entity, config in SILVER_CONFIG.items():
        print(f"--- Processing {entity} ---")
        bronze_df = spark.table(f"local.bronze.{entity}")
        business_cols = [c for c in bronze_df.columns if c not in TECHNICAL_COLS]

        # Deduplicate on business columns only -- catches our injected duplicate events,
        # since a true duplicate has identical business data but different Kafka offset/timestamp
        deduped = bronze_df.dropDuplicates(business_cols)

        required = config["required"]
        null_condition = reduce(lambda a, b: a | b, [col(r).isNull() for r in required])

        quarantine_df = deduped.filter(null_condition)
        clean_df = deduped.filter(~null_condition)

        bronze_count = bronze_df.count()
        deduped_count = deduped.count()
        quarantine_count = quarantine_df.count()

        print(f"  bronze: {bronze_count} -> after dedup: {deduped_count} -> "
              f"clean: {deduped_count - quarantine_count}, quarantined: {quarantine_count}")

        clean_df.drop(*TECHNICAL_COLS).writeTo(f"local.silver.{entity}").createOrReplace()
        if quarantine_count > 0:
            quarantine_df.writeTo(f"local.quarantine.{entity}").createOrReplace()

    print("Silver transformation complete for all entities.")
    spark.stop()
    os._exit(0)

if __name__ == "__main__":
    main()