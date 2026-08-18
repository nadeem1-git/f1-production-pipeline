import os
from spark_utils import get_spark_session

spark = get_spark_session("investigate-failures")

laps = spark.table("local.silver.laps")
print("laps total:", laps.count())
print("laps with null lap_duration:", laps.filter("lap_duration IS NULL").count())
print("laps with lap_duration > 600:", laps.filter("lap_duration > 600").count())
laps.filter("lap_duration > 600").select("session_key", "driver_number", "lap_number", "lap_duration", "is_pit_out_lap").show(5)

pits = spark.table("local.silver.pit_stops")
print("pit_stops total:", pits.count())
print("pit_stops with null pit_duration:", pits.filter("pit_duration IS NULL").count())
print("pit_stops with pit_duration > 300:", pits.filter("pit_duration > 300").count())
pits.filter("pit_duration > 300").select("session_key", "driver_number", "lap_number", "pit_duration", "stop_duration").show(5)

spark.stop()
os._exit(0)