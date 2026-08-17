import os
from spark_utils import get_spark_session

spark = get_spark_session("verify-bronze")

df = spark.table("local.bronze.laps")
print(f"Total rows in local.bronze.laps: {df.count()}")
df.show(5, truncate=False)

spark.stop()
os._exit(0)