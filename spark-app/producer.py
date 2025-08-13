import io
import json
import os
import struct
import time
from typing import Tuple

import requests
from fastavro import parse_schema, schemaless_writer
from confluent_kafka import Producer


REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "demo-topic")
SCHEMA_PATH = os.environ.get("AVRO_SCHEMA", "greeting.avsc")
SUBJECT = os.environ.get("SCHEMA_SUBJECT", f"{TOPIC}-value")


def registry_base(url: str) -> str:
    url = url.rstrip('/')
    if '/apis/ccompat/' not in url:
        url = url + '/apis/ccompat/v7'
    return url


def register_schema(schema_str: str) -> int:
    base = registry_base(REGISTRY_URL)
    # Confluent-compatible endpoint
    endpoint = f"{base}/subjects/{SUBJECT}/versions"
    payload = {"schema": schema_str, "schemaType": "AVRO"}
    resp = requests.post(endpoint, json=payload, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    schema_id = data.get("id")
    if schema_id is None:
        raise RuntimeError(f"Unexpected registry response: {data}")
    return int(schema_id)


def build_confluent_payload(schema_id: int, record: dict, parsed_schema: dict) -> bytes:
    # Confluent wire format: magic byte (0) + 4-byte schema id (big endian) + avro payload
    out = io.BytesIO()
    out.write(b"\x00")
    out.write(struct.pack(">I", int(schema_id)))
    schemaless_writer(out, parsed_schema, record)
    return out.getvalue()


def main():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_str = f.read()
    schema_json = json.loads(schema_str)
    parsed = parse_schema(schema_json)

    schema_id = register_schema(json.dumps(schema_json))
    print(f"Registered subject '{SUBJECT}' with id {schema_id}")

    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    # Example record matching greeting.avsc
    example = {"message": "Hello from producer", "timestamp": int(time.time() * 1000)}

    payload = build_confluent_payload(schema_id, example, parsed)
    def delivery(err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Produced to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")
    producer.produce(TOPIC, value=payload, on_delivery=delivery)
    producer.flush(10)


if __name__ == "__main__":
    main()
