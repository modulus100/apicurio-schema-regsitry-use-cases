package org.example.app;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;

public class FlinkKafkaAvroApp {
    private static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final String TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "demo-topic");
    // Apicurio Registry exposes Confluent-compatible API under /apis/ccompat/v7
    private static final String SCHEMA_REGISTRY_URL = System.getenv().getOrDefault(
            "SCHEMA_REGISTRY_URL",
            "http://localhost:8081/apis/ccompat/v7"
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Load the reader schema from resources (greeting.avsc)
        Schema readerSchema = loadSchema("/avro/greeting.avsc");

        DeserializationSchema<GenericRecord> valueDeser =
                ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, SCHEMA_REGISTRY_URL);

        KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC)
                .setGroupId("flink-avro-demo-consumer-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(valueDeser)
                .build();

        DataStreamSource<GenericRecord> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-avro-source"
        );

        stream.map(record -> {
            Object msg = record.get("message");
            Object ts = record.get("timestamp");
            String message = Objects.toString(msg, "<null>");
            long timestamp = ts instanceof Long ? (Long) ts : 0L;
            return "Greeting{message='" + message + "', timestamp=" + timestamp +
                    ", iso='" + Instant.ofEpochMilli(timestamp) + "'}";
        }).print();

        env.execute("Flink Kafka Avro Demo");
    }

    private static Schema loadSchema(String resourcePath) {
        try (InputStream is = FlinkKafkaAvroApp.class.getResourceAsStream(resourcePath)) {
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
