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

package io.zyient.base.core.connections.azure;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.MessageConnection;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.azure.AzureServiceBusConnectionSettings;
import io.zyient.base.core.keystore.KeyStore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public abstract class ServiceBusConnection extends MessageConnection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    protected ServiceBusConnectionConfig config;
    protected BaseEnv<?> env;
    protected KeyStore keyStore;

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public void read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                     @NonNull BaseEnv<?> env) throws Exception {
        this.env = env;
        keyStore = env.keyStore();
        Preconditions.checkNotNull(keyStore);

        config = new ServiceBusConnectionConfig(xmlConfig);
        config.read();
        settings = (AzureServiceBusConnectionSettings) config.settings();
        settings.validate();
    }

    public void read(@NonNull String name,
                     @NonNull ZookeeperConnection connection,
                     @NonNull String path,
                     @NonNull BaseEnv<?> env) throws Exception {
        this.env = env;
        keyStore = env.keyStore();
        Preconditions.checkNotNull(keyStore);

        CuratorFramework client = connection.client();
        String zkPath = new PathUtils.ZkPathBuilder(path)
                .withPath(ServiceBusConnectionConfig.__CONFIG_PATH)
                .build();
        ZkConfigReader reader = new ZkConfigReader(client, AzureServiceBusConnectionSettings.class);
        if (!reader.read(zkPath)) {
            throw new ConnectionError(
                    String.format("Chronicle Connection settings not found. [path=%s]", zkPath));
        }
        settings = (AzureServiceBusConnectionSettings) reader.settings();
        settings.validate();
    }

    protected String getConnectionString() throws Exception {
        return keyStore.read(((AzureServiceBusConnectionSettings) settings).getConnectionString());
    }

    @Override
    public Throwable error() {
        return state.getError();
    }

    @Override
    public EConnectionState connectionState() {
        return state.getState();
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.servicebus;
    }

    @Override
    public String path() {
        return ServiceBusConnectionConfig.__CONFIG_PATH;
    }

    public static class ServiceBusConnectionConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "azure.messaging";

        public ServiceBusConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, AzureServiceBusConnectionSettings.class);
        }

        public ServiceBusConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                          @NonNull String path,
                                          @NonNull Class<? extends AzureServiceBusConnectionSettings> settingsType) {
            super(config, path, settingsType);
        }
    }

}
