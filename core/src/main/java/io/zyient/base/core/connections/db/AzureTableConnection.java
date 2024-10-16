/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.connections.db;

import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionConfig;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.db.AzureTableConnectionSettings;
import io.zyient.base.core.keystore.KeyStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureTableConnection implements Connection {
    private AzureTableConnectionConfig config;
    private AzureTableConnectionSettings settings;
    protected ConnectionManager connectionManager;
    private TableServiceClient client;
    private final ConnectionState state = new ConnectionState();

    @Override
    public String name() {
        return settings.getName();
    }

    public String db() {
        return settings.getDb();
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                this.connectionManager = env.connectionManager();
                config = new AzureTableConnectionConfig(xmlConfig);
                config.read();
                settings = (AzureTableConnectionSettings) config.settings();

                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                CuratorFramework client = connection.client();
                String zkPath = new PathUtils.ZkPathBuilder(path)
                        .withPath(AzureTableConnectionConfig.__CONFIG_PATH)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, AzureTableConnectionSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("JDBC Connection settings not found. [path=%s]", zkPath));
                }
                settings = (AzureTableConnectionSettings) reader.settings();
                settings.validate();

                this.connectionManager = env.connectionManager();
                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                this.settings = (AzureTableConnectionSettings) settings;
                this.connectionManager = env.connectionManager();
                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        if (state.getState() != EConnectionState.Initialized) {
            throw new ConnectionError(String.format("[%s] Not initialized.", name()));
        }
        KeyStore keyStore = connectionManager().keyStore();
        Preconditions.checkNotNull(keyStore);
        try {
            synchronized (state) {
                String cs = keyStore.read(settings.getConnectionString());
                client = new TableServiceClientBuilder()
                        .connectionString(cs)
                        .buildClient();
                state.setState(EConnectionState.Connected);
                return this;
            }
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
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
    public String path() {
        return null;
    }

    @Override
    public ConnectionSettings settings() {
        return settings;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (client != null) {
                client = null;
            }
            if (state.isConnected()) {
                state.setState(EConnectionState.Closed);
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class AzureTableConnectionConfig extends ConnectionConfig {
        public static final String __CONFIG_PATH = "azure.table";

        public AzureTableConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config,
                    __CONFIG_PATH,
                    AzureTableConnectionSettings.class,
                    AzureTableConnection.class);
        }
    }
}
