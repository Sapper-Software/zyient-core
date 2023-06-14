package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.kafka.KafkaConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <pre>
 *     <connections>
 *         <connection>
 *             <class>[Connection class]</class>
 *             <kafka>
 *                  <name>[Connection name, must be unique]</name>
 *                  <Mode>[Producer|Consumer]</Mode>
 *                  <config>[Path to config file. [Local Path or URL]</config>
 *                  <consumer>
 *                      <partitions>[; seperated list of partitions (optional)]</partitions>
 *                  </consumer>
 *                  <topic>[Topic name]</topic>
 *             </kafka>
 *         </connection>
 *     </connections>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaSettings extends ConnectionSettings {
    public static final String PROP_CLIENT_ID = "client.id";
    public static class Constants {
        public static final String CONFIG_MODE = "mode";
        public static final String CONFIG_FILE_CONFIG = "config";
        public static final String CONFIG_PARTITIONS = "consumer.partitions";
        public static final String CONFIG_TOPIC = "topic";
    }

    @Config(name = Constants.CONFIG_FILE_CONFIG)
    private String configPath;
    private Properties properties;
    @Config(name = Constants.CONFIG_MODE, required = false, type = KafkaConnection.EKafkaClientMode.class)
    private KafkaConnection.EKafkaClientMode mode = KafkaConnection.EKafkaClientMode.Producer;
    @Config(name = Constants.CONFIG_TOPIC, required = false)
    private String topic;
    @Config(name = Constants.CONFIG_PARTITIONS, required = false, type = List.class, parser = KafkaPartitionsParser.class)
    private List<Integer> partitions;

    public KafkaSettings() {
        setType(EConnectionType.kafka);
    }

    public KafkaSettings(@NonNull KafkaSettings settings) {
        super(settings);
        setType(EConnectionType.kafka);
        configPath = settings.configPath;
        if (settings.properties != null) {
            properties = new Properties(settings.properties.size());
            properties.putAll(settings.properties);
        }
        mode = settings.mode;
        topic = settings.topic;
        if (settings.partitions != null) {
            partitions = new ArrayList<>(settings.partitions);
        }
    }

    public KafkaSettings clientId(@NonNull String clientId) {
        properties.put(PROP_CLIENT_ID, clientId);
        return this;
    }

    public String clientId() {
        if (properties != null) {
            return properties.getProperty(PROP_CLIENT_ID);
        }
        return null;
    }
}
