package org.example.app.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.io.IOException;
import java.io.InputStream;

@Service
public class ProducerService {
    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final Schema greetingSchema;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic:demo-topic}")
    private String topic;

    public ProducerService(
            KafkaTemplate<String, Object> kafkaTemplate,
            ResourceLoader resourceLoader,
            @Value("${app.kafka.avro.schema-location:classpath:avro/greeting.avsc}") String schemaLocation
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.greetingSchema = loadSchema(resourceLoader, schemaLocation);
    }

    private Schema loadSchema(ResourceLoader resourceLoader, String location) {
        Resource resource = resourceLoader.getResource(location);
        try (InputStream is = resource.getInputStream()) {
            return new Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Avro schema from: " + location, e);
        }
    }

    // Send a message every 5 seconds
    @Scheduled(fixedDelay = 5000, initialDelay = 3000)
    public void sendMessage() {
        GenericRecord record = new GenericData.Record(greetingSchema);
        String message = "Hello from Spring Boot (Avro)";
        long ts = Instant.now().toEpochMilli();
        record.put("message", message + " @ " + ts);
        record.put("timestamp", ts);

        kafkaTemplate.send(topic, record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send Avro record", ex);
            } else if (result != null && result.getRecordMetadata() != null) {
                log.info("Sent Avro record to {}-{}@{}", 
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.info("Sent Avro record");
            }
        });
    }
}
