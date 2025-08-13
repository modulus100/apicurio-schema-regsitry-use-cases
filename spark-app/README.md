# PySpark Kafka Avro (local, no cluster)

This project runs a local PySpark Structured Streaming app that reads Avro-encoded messages from Kafka using Confluent wire format and Apicurio Registry (Confluent-compatible API). It logs the decoded records to the console.

## Prereqs
- macOS with Python 3.12
- Java 11 or 17 (e.g. Temurin/OpenJDK). Spark 3.5.x supports Java 8/11/17.
- Docker and Docker Compose

## Services
`docker-compose.yml` starts:
- Kafka broker on `localhost:9092`
- Apicurio Registry on `http://localhost:8081` (Confluent-compat API enabled)

## Setup
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Start Kafka + Apicurio Registry
docker compose up -d
# Optional: wait for health
until curl -fsS http://localhost:8081/apis/ccompat/v7/subjects >/dev/null; do sleep 2; done
```

## Produce a test Avro message
`greeting.avsc`:
```json
{
  "type": "record",
  "name": "Greeting",
  "namespace": "org.example.avro",
  "fields": [
    { "name": "message", "type": "string" },
    { "name": "timestamp", "type": "long" }
  ]
}
```
Run the producer (registers the schema and sends one record in Confluent wire format):
```bash
python producer.py
```
Environment variables (optional):
- `SCHEMA_REGISTRY_URL` (default `http://localhost:8081`)
- `KAFKA_BOOTSTRAP` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `demo-topic`)
- `AVRO_SCHEMA` (default `greeting.avsc`)
- `SCHEMA_SUBJECT` (default `<topic>-value`)

## Run the PySpark streaming app
You can run with Python directly (PySpark is a dependency):
```bash
python main.py
```
Or with spark-submit (optional):
```bash
python - <<'PY'
import os
from pyspark.sql import SparkSession
spark = (SparkSession.builder
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
  .getOrCreate())
spark.stop()
print("Spark OK")
PY

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 main.py
```

The app prints decoded JSON for each Kafka record to the console.

## Troubleshooting
- Java not found: install a JDK (11 or 17) and ensure `JAVA_HOME` is set.
- Cannot connect to Kafka: ensure `docker compose ps` shows the `kafka` service healthy and that `KAFKA_ADVERTISED_LISTENERS` is `PLAINTEXT://localhost:9092` (already set).
- Schema decode returns null: make sure messages are produced in Confluent wire format (magic byte 0 + 4-byte schema ID + Avro payload). Use `producer.py`.
- Different schema: adjust the example payload in `producer.py` to match your `.avsc`.

## Env Vars used by the app
- `KAFKA_TOPIC` (default `demo-topic`)
- `KAFKA_BOOTSTRAP` (default `localhost:9092`)
- `SCHEMA_REGISTRY_URL` (default `http://localhost:8081`)
