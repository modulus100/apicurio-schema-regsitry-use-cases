package org.example.connect.logger;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

public class LoggerSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(LoggerSinkTask.class);

    private String prefix;
    private String level;

    @Override
    public void start(Map<String, String> props) {
        this.prefix = props.getOrDefault(LoggerSinkConnector.LOG_PREFIX_CONFIG, "");
        this.level = props.getOrDefault(LoggerSinkConnector.LOG_LEVEL_CONFIG, "INFO").toUpperCase();
        log.info("LoggerSinkTask started with prefix='{}', level='{}'", prefix, level);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord r : records) {
            String keyStr = toPrintable(r.key());
            String valStr = toPrintable(r.value());
            String msg = String.format("%s topic=%s partition=%d offset=%d key=%s value=%s", prefix,
                    r.topic(), r.kafkaPartition(), r.kafkaOffset(), keyStr, valStr);
            switch (level) {
                case "TRACE":
                    log.trace(msg);
                    break;
                case "DEBUG":
                    log.debug(msg);
                    break;
                case "WARN":
                    log.warn(msg);
                    break;
                case "ERROR":
                    log.error(msg);
                    break;
                default:
                    log.info(msg);
            }
        }
    }

    private String toPrintable(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }
        return String.valueOf(obj);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // nothing special
    }

    @Override
    public void stop() {
        log.info("LoggerSinkTask stopping");
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion() == null ? "0.1.0" : getClass().getPackage().getImplementationVersion();
    }
}
