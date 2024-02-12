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

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.db.ClickhouseConnectionSettings;
import io.zyient.base.core.keystore.KeyStore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class ClickhouseConnection implements Connection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    protected final String zkNode;
    @Getter(AccessLevel.NONE)
    private ClickHouseDataSource dataSource;
    private ClickhouseConnectionConfig config;
    private ClickhouseConnectionSettings settings;
    private ConnectionManager connectionManager;
    private BaseEnv<?> env;

    public ClickhouseConnection() {
        zkNode = ClickhouseConnectionSettings.__CONFIG_PATH;
    }

    private String createUrl() {
        StringBuilder builder = new StringBuilder("jdbc:clickhouse:");
        builder.append(settings.getProtocol())
                .append("://");
        for (int ii = 0; ii < settings.getDbUrls().size(); ii++) {
            if (ii > 0) {
                builder.append(",");
            }
            builder.append(settings.getDbUrls().get(ii));
        }
        if (settings.getPort() > 0) {
            builder.append(":").append(settings.getPort());
        }
        builder.append("/").append(settings.getDb());
        if (settings.isUseSSL()) {
            builder.append("?");
            builder.append("ssl=true");
            builder.append("&sslMode=").append(settings.getSslMode());
        }
        return builder.toString();
    }

    @Override
    public String name() {
        return null;
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
                this.env = env;
                this.connectionManager = env.connectionManager();
                config = new ClickhouseConnectionConfig(xmlConfig);
                config.read();
                settings = (ClickhouseConnectionSettings) config.settings();

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
                this.env = env;
                CuratorFramework client = connection.client();
                String zkPath = new PathUtils.ZkPathBuilder(path)
                        .withPath(zkNode)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, ClickhouseConnectionSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("ClickHouse Connection settings not found. [path=%s]", zkPath));
                }
                settings = (ClickhouseConnectionSettings) reader.settings();
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
                this.env = env;
                this.settings = (ClickhouseConnectionSettings) settings;
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
        if (!isConnected()) {
            try {
                state.check(Connection.EConnectionState.Initialized);
                String url = createUrl();
                Properties props = new Properties();
                if (settings.getParameters() != null) {
                    props.putAll(settings.getParameters());
                }
                dataSource = new ClickHouseDataSource(url, props);
                state.setState(Connection.EConnectionState.Connected);
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                state.error(ex);
                throw new ConnectionError(ex);
            }
        } else {
            state.check(Connection.EConnectionState.Connected);
        }
        return this;
    }

    @Override
    public Throwable error() {
        if (state.hasError()) {
            return state.getError();
        }
        return null;
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
        return ClickhouseConnectionSettings.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.db;
    }

    public ClickHouseConnection client() throws ConnectionError {
        state.check(Connection.EConnectionState.Connected);
        try {
            String user = settings.getUsername();
            String passwd = settings.getPassKey();
            if (!Strings.isNullOrEmpty(passwd)) {
                KeyStore keyStore = env.keyStore();
                if (keyStore == null) {
                    throw new ConnectionError(String.format("[env=%s] Key Store not specified...", env.name()));
                }
                passwd = keyStore.read(passwd);
                if (Strings.isNullOrEmpty(passwd)) {
                    throw new ConnectionError(String.format("Pass Key not found. [key=%s]", settings.getPassKey()));
                }
            }
            return dataSource.getConnection(user, passwd);
        } catch (Exception e) {
            throw new ConnectionError(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(Connection.EConnectionState.Closed);
        }
        if (dataSource != null) {
            dataSource = null;
        }
    }
}
