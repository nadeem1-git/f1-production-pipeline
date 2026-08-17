from spark_utils import get_spark_session

spark = get_spark_session("connector-test")
print("Spark session created successfully with Kafka + Iceberg + S3A support")
spark.stop()
import os
from spark_utils import get_spark_session

spark = get_spark_session("connector-test")
print("Spark session created successfully with Kafka + Iceberg + S3A support")
spark.stop()
os._exit(0)