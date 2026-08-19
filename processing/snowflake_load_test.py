import os
from spark_utils import get_spark_session
from snowflake_config import SNOWFLAKE_CONFIG

spark = get_spark_session("snowflake-load-test")

df = spark.table("local.silver.drivers")
print(f"Loading {df.count()} drivers into Snowflake...")

(df.write
    .format("net.snowflake.spark.snowflake")
    .options(**SNOWFLAKE_CONFIG)
    .option("dbtable", "DRIVERS")
    .mode("overwrite")
    .save())

print("Drivers loaded to Snowflake successfully.")
spark.stop()
os._exit(0)