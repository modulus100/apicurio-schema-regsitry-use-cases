import json
import os
from typing import Optional

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_json
from pyspark.sql.avro.functions import from_avro
import sys


def _normalize_registry_url(url: str) -> str:
    url = url.rstrip("/")
    # Apicurio Confluent-compat API path
    if not url.endswith("/apis/ccompat/v7"):
        url = f"{url}/apis/ccompat/v7"
    return url


def build_spark(app_name: str) -> SparkSession:
    # Spark 3.5.1 supports Python 3.12. Pull Kafka connector via maven coordinates.
    # Ensure workers use the same Python as the driver
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.spark:spark-avro_2.12:3.5.1",
            ]),
        )
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    # Reduce logs
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    topic = os.environ.get("KAFKA_TOPIC", "demo-topic")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    subject = os.environ.get("SCHEMA_SUBJECT", f"{topic}-value")
    schema_file = os.environ.get("AVRO_SCHEMA", "greeting.avsc")

    spark = build_spark("KafkaAvroConsumer")

    # Load Avro schema once (prefer local file; otherwise fetch latest from Schema Registry)
    schema_str: Optional[str] = None
    try:
        if schema_file and os.path.exists(schema_file):
            with open(schema_file, "r", encoding="utf-8") as f:
                schema_str = f.read()
        else:
            client = SchemaRegistryClient({"url": _normalize_registry_url(registry_url)})
            latest = client.get_latest_version(subject)
            schema_str = latest.schema.schema_str
    except Exception as e:
        print(f"Failed to load schema: {e}")
        raise

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Drop the 5-byte Confluent wire header and decode Avro payload using Spark Avro (JVM)
    df2 = df.select(
        col("topic"), col("partition"), col("offset"), col("timestamp"),
        expr("substring(value, 6)").alias("avro_payload")
    )

    decoded_struct = df2.select(
        "topic", "partition", "offset", "timestamp",
        from_avro(col("avro_payload"), schema_str).alias("record")
    )

    decoded = decoded_struct.select(
        "topic", "partition", "offset", "timestamp",
        to_json(col("record")).alias("record_json")
    )

    query = (
        decoded.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

