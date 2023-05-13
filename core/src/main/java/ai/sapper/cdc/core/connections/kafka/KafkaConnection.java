package ai.sapper.cdc.core.connections.kafka;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.*;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.KafkaSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public abstract class KafkaConnection implements MessageConnection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();

    private KafkaConfig kafkaConfig;
    private KafkaSettings settings;

    /**
     * @return
     */
    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear(EConnectionState.Unknown);
            kafkaConfig = new KafkaConfig(xmlConfig);
            kafkaConfig.read();
            settings = (KafkaSettings) kafkaConfig.settings();
            if (Strings.isNullOrEmpty(settings.clientId()))
                settings.clientId(env.moduleInstance().getInstanceId());
        } catch (Throwable t) {
            state.error(t);
            throw new ConnectionError("Error opening HDFS connection.", t);
        }
        return this;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear(EConnectionState.Unknown);

            CuratorFramework client = connection.client();
            String zkPath = new PathUtils.ZkPathBuilder(path)
                    .withPath(KafkaConfig.__CONFIG_PATH)
                    .build();
            ZkConfigReader reader = new ZkConfigReader(client, KafkaSettings.class);
            if (!reader.read(zkPath)) {
                throw new ConnectionError(
                        String.format("Kafka Connection settings not found. [path=%s]", zkPath));
            }
            settings = (KafkaSettings) reader.settings();
            settings.validate();

            if (Strings.isNullOrEmpty(settings.clientId()))
                settings.clientId(env.moduleInstance().getInstanceId());
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof KafkaSettings);
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear(EConnectionState.Unknown);
            this.settings = (KafkaSettings) settings;
            if (Strings.isNullOrEmpty(((KafkaSettings) settings).clientId()))
                ((KafkaSettings) settings).clientId(env.moduleInstance().getInstanceId());
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
        return this;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.error();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.state();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return (state.isConnected());
    }

    public String topic() {
        Preconditions.checkState(settings != null);
        return settings.getTopic();
    }

    @Override
    public String path() {
        return KafkaConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
    }

    public enum EKafkaClientMode {
        Producer, Consumer
    }


    @Getter
    @Accessors(fluent = true)
    public static class KafkaConfig extends ConnectionConfig {
        private static final String __CONFIG_PATH = "kafka";

        public KafkaConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, KafkaSettings.class);
        }

        public void read() throws ConfigurationException {
            super.read();
            try {
                KafkaSettings settings = (KafkaSettings) settings();

                if (settings.getMode() == EKafkaClientMode.Producer) {
                    File configFile = ConfigReader.readFileNode(get(), KafkaSettings.Constants.CONFIG_PRODUCER_CONFIG);
                    if (configFile == null || !configFile.exists()) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                                KafkaSettings.Constants.CONFIG_FILE_CONFIG));
                    }
                    settings.setConfigPath(get().getString(KafkaSettings.Constants.CONFIG_PRODUCER_CONFIG));
                    settings.setProperties(new Properties());
                    settings.getProperties().load(new FileInputStream(configFile));
                } else if (settings.getMode() == EKafkaClientMode.Consumer) {
                    HierarchicalConfiguration<ImmutableNode> cnode = get().configurationAt(KafkaSettings.Constants.CONFIG_CONSUMER);
                    if (cnode == null) {
                        throw new ConfigurationException(
                                String.format("Invalid Consumer configuration: missing path. [path=%s]",
                                        KafkaSettings.Constants.CONFIG_CONSUMER));
                    }
                    File configFile = ConfigReader.readFileNode(cnode, KafkaSettings.Constants.CONFIG_FILE_CONFIG);
                    if (configFile == null || !configFile.exists()) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                                KafkaSettings.Constants.CONFIG_FILE_CONFIG));
                    }
                    settings.setConfigPath(cnode.getString(KafkaSettings.Constants.CONFIG_FILE_CONFIG));
                    settings.setProperties(new Properties());
                    settings.getProperties().load(new FileInputStream(configFile));

                    if (settings.getPartitions().isEmpty()) {
                        settings.getPartitions().add(0);
                    }
                }
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing Kafka configuration.", t);
            }
        }
    }
}
