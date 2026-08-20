import os
from pyspark.sql.functions import to_json, col
from pyspark.sql.types import ArrayType
from spark_utils import get_spark_session
from snowflake_config import SNOWFLAKE_CONFIG

TABLES = ["laps", "meetings", "positions", "pit_stops", "stints", "weather", "race_control", "team_radio", "sessions"]

def flatten_arrays(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, to_json(col(field.name)))
    return df

def main():
    spark = get_spark_session("snowflake-load-all")

    for entity in TABLES:
        print(f"--- Loading {entity} ---")
        df = spark.table(f"local.silver.{entity}")
        df = flatten_arrays(df)
        count = df.count()

        (df.write
            .format("net.snowflake.spark.snowflake")
            .options(**SNOWFLAKE_CONFIG)
            .option("dbtable", entity.upper())
            .mode("overwrite")
            .save())

        print(f"{entity}: {count} rows loaded")

    print("All Silver tables loaded into Snowflake.")
    spark.stop()
    os._exit(0)

if __name__ == "__main__":
    main()