import os
from pyspark.sql.functions import col, from_json, current_timestamp
from spark_utils import get_spark_session
from bronze_schemas import ENTITIES

TYPE_MAP = {
    "integer": "INT",
    "double": "DOUBLE",
    "string": "STRING",
    "boolean": "BOOLEAN",
}

def build_ddl_columns(schema):
    cols = [f"{f.name} {TYPE_MAP[f.dataType.typeName()]}" for f in schema.fields]
    cols += ["kafka_partition INT", "kafka_offset LONG", "kafka_timestamp TIMESTAMP", "ingested_at TIMESTAMP"]
    return ", ".join(cols)

def main():
    spark = get_spark_session("bronze-remaining-entities")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

    for topic, config in ENTITIES.items():
        table = config["table"]
        schema = config["schema"]
        print(f"--- Processing {topic} -> {table} ---")

        ddl_cols = build_ddl_columns(schema)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl_cols}) USING iceberg")

        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed = (
            raw
            .select(
                from_json(col("value").cast("string"), schema).alias("data"),
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
            .option("checkpointLocation", f"s3a://f1-lakehouse/checkpoints/bronze_{topic}")
            .toTable(table)
        )
        query.awaitTermination()
        print(f"{topic}: done")

    print("All remaining entities loaded into Bronze.")
    spark.stop()
    os._exit(0)

if __name__ == "__main__":
    main()