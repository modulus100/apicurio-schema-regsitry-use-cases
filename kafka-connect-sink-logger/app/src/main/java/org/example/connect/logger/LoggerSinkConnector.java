package org.example.connect.logger;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal SinkConnector that just logs incoming records in its task.
 */
public class LoggerSinkConnector extends SinkConnector {

    public static final String LOG_LEVEL_CONFIG = "log.level";
    public static final String LOG_PREFIX_CONFIG = "log.prefix";

    private Map<String, String> originalProps = new HashMap<>();

    @Override
    public void start(Map<String, String> props) {
        this.originalProps = new HashMap<>(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LoggerSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(originalProps));
        }
        return configs;
    }

    @Override
    public void stop() {
        // nothing to do
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(LOG_LEVEL_CONFIG, ConfigDef.Type.STRING, "INFO", ConfigDef.Importance.LOW,
                        "Log level used by the logger task (TRACE, DEBUG, INFO, WARN, ERROR)")
                .define(LOG_PREFIX_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                        "Optional prefix added to each logged record");
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion() == null ? "0.1.0" : getClass().getPackage().getImplementationVersion();
    }
}
