package org.example.app;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Future;

public class AvroProducer {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "demo-topic");
        String registryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7");

        Schema schema = loadSchema("/avro/greeting.avsc");

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", registryUrl);
        props.put("auto.register.schemas", "true");
        props.put("use.latest.version", "true");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("message", "Hello from Avro #" + i);
                record.put("timestamp", System.currentTimeMillis());

                ProducerRecord<String, GenericRecord> pr = new ProducerRecord<>(topic, "key-" + i, record);
                Future<RecordMetadata> fut = producer.send(pr);
                RecordMetadata md = fut.get();
                System.out.printf("Produced to %s-%d@%d%n", md.topic(), md.partition(), md.offset());
                Thread.sleep(200);
            }
            producer.flush();
        }
    }

    private static Schema loadSchema(String resourcePath) {
        try (InputStream is = AvroProducer.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("Schema resource not found: " + resourcePath);
            }
            String schemaStr = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(schemaStr);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Avro schema from " + resourcePath, e);
        }
    }
}
