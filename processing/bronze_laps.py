from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, BooleanType, ArrayType
from pyspark.sql.functions import col, from_json, current_timestamp
from spark_utils import get_spark_session

LAPS_SCHEMA = StructType([
    StructField("meeting_key", IntegerType()),
    StructField("session_key", IntegerType()),
    StructField("driver_number", IntegerType()),
    StructField("lap_number", IntegerType()),
    StructField("date_start", StringType()),
    StructField("duration_sector_1", DoubleType()),
    StructField("duration_sector_2", DoubleType()),
    StructField("duration_sector_3", DoubleType()),
    StructField("i1_speed", IntegerType()),
    StructField("i2_speed", IntegerType()),
    StructField("is_pit_out_lap", BooleanType()),
    StructField("lap_duration", DoubleType()),
    StructField("st_speed", IntegerType()),
    StructField("segments_sector_1", ArrayType(IntegerType())),
    StructField("segments_sector_2", ArrayType(IntegerType())),
    StructField("segments_sector_3", ArrayType(IntegerType())),
])

def main():
    spark = get_spark_session("bronze-laps")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.laps (
            meeting_key INT, session_key INT, driver_number INT, lap_number INT,
            date_start STRING, duration_sector_1 DOUBLE, duration_sector_2 DOUBLE,
            duration_sector_3 DOUBLE, i1_speed INT, i2_speed INT,
            is_pit_out_lap BOOLEAN, lap_duration DOUBLE, st_speed INT,
            segments_sector_1 ARRAY<INT>, segments_sector_2 ARRAY<INT>, segments_sector_3 ARRAY<INT>,
            kafka_partition INT, kafka_offset LONG, kafka_timestamp TIMESTAMP, ingested_at TIMESTAMP
        ) USING iceberg
    """)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "laps")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw
        .select(
            from_json(col("value").cast("string"), LAPS_SCHEMA).alias("data"),
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
        .option("checkpointLocation", "s3a://f1-lakehouse/checkpoints/bronze_laps")
        .toTable("local.bronze.laps")
    )

    query.awaitTermination()
    print("Bronze laps batch load complete.")

if __name__ == "__main__":
    main()