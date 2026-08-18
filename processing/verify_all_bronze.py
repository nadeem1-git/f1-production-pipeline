import os
from spark_utils import get_spark_session

spark = get_spark_session("verify-all-bronze")

tables = ["laps", "meetings", "drivers", "positions", "pit_stops", "stints", "weather", "race_control", "team_radio"]

for t in tables:
    count = spark.table(f"local.bronze.{t}").count()
    print(f"{t}: {count} rows")

spark.stop()
os._exit(0)