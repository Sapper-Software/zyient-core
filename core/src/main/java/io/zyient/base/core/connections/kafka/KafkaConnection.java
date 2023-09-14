/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.core.connections.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.*;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.kafka.KafkaSettings;
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
import java.util.ArrayList;
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
            state.clear();
            kafkaConfig = new KafkaConfig(xmlConfig, getClass());
            kafkaConfig.read();
            settings = (KafkaSettings) kafkaConfig.settings();
            return this;
        } catch (Throwable t) {
            state.error(t);
            throw new ConnectionError("Error opening HDFS connection.", t);
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
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
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof KafkaSettings);
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear();
            this.settings = (KafkaSettings) settings;
            if (Strings.isNullOrEmpty(((KafkaSettings) settings).clientId()))
                ((KafkaSettings) settings).clientId(env.moduleInstance().getInstanceId());
            state.setState(EConnectionState.Initialized);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
        return this;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.getError();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.getState();
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


    @Getter
    @Accessors(fluent = true)
    public static class KafkaConfig extends ConnectionConfig {
        private static final String __CONFIG_PATH = "kafka";

        public KafkaConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull Class<? extends KafkaConnection> connectionClass) {
            super(config, __CONFIG_PATH, KafkaSettings.class, connectionClass);
        }

        public void read() throws ConfigurationException {
            super.read();
            try {
                KafkaSettings settings = (KafkaSettings) settings();

                if (settings.getMode() == EMessageClientMode.Producer) {
                    File configFile = ConfigReader.readFileNode(settings.getConfigPath());
                    if (configFile == null || !configFile.exists()) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                                KafkaSettings.Constants.CONFIG_FILE_CONFIG));
                    }
                    settings.setProperties(new Properties());
                    settings.getProperties().load(new FileInputStream(configFile));
                } else if (settings.getMode() == EMessageClientMode.Consumer) {
                    File configFile = ConfigReader.readFileNode(settings.getConfigPath());
                    if (configFile == null || !configFile.exists()) {
                        throw new ConfigurationException(String.format("Kafka Configuration Error: missing [%s]",
                                KafkaSettings.Constants.CONFIG_FILE_CONFIG));
                    }
                    settings.setProperties(new Properties());
                    settings.getProperties().load(new FileInputStream(configFile));
                    if (settings.getPartitions() == null) {
                        settings.setPartitions(new ArrayList<>());
                    }
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
