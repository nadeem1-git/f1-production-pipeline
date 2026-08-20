import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.functions import col, from_json, current_timestamp
from spark_utils import get_spark_session

SESSIONS_SCHEMA = StructType([
    StructField("session_key", IntegerType()),
    StructField("session_type", StringType()),
    StructField("session_name", StringType()),
    StructField("date_start", StringType()),
    StructField("date_end", StringType()),
    StructField("meeting_key", IntegerType()),
    StructField("circuit_key", IntegerType()),
    StructField("circuit_short_name", StringType()),
    StructField("country_key", IntegerType()),
    StructField("country_code", StringType()),
    StructField("country_name", StringType()),
    StructField("location", StringType()),
    StructField("gmt_offset", StringType()),
    StructField("year", IntegerType()),
    StructField("is_cancelled", BooleanType()),
])

def main():
    spark = get_spark_session("bronze-sessions")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.sessions (
            session_key INT, session_type STRING, session_name STRING,
            date_start STRING, date_end STRING, meeting_key INT,
            circuit_key INT, circuit_short_name STRING, country_key INT,
            country_code STRING, country_name STRING, location STRING,
            gmt_offset STRING, year INT, is_cancelled BOOLEAN,
            kafka_partition INT, kafka_offset LONG, kafka_timestamp TIMESTAMP, ingested_at TIMESTAMP
        ) USING iceberg
    """)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "sessions")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw
        .select(
            from_json(col("value").cast("string"), SESSIONS_SCHEMA).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingested_at", current_timestamp())
    )

    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", "s3a://f1-lakehouse/checkpoints/bronze_sessions")
        .toTable("local.bronze.sessions")
    )
    query.awaitTermination()
    print("Bronze sessions load complete.")
    spark.stop()
    os._exit(0)

if __name__ == "__main__":
    main()