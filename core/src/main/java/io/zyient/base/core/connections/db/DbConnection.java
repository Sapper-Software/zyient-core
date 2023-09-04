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

package io.zyient.base.core.connections.db;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.JdbcConnectionSettings;
import io.zyient.base.core.keystore.KeyStore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.curator.framework.CuratorFramework;

import java.sql.SQLException;

@Getter
@Accessors(fluent = true)
public abstract class DbConnection implements Connection {
    public static class Constants {
        public static final String DB_KEY_USER = "user=";
        public static final String DB_KEY_PASSWD = "password=";
        public static final String DB_KEY_POOL_SIZE = "maxPoolSize=";
    }


    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    protected JdbcConnectionSettings settings;
    protected ConnectionManager connectionManager;
    protected final String zkNode;

    public DbConnection(@NonNull String zkNode) {
        this.zkNode = zkNode;
    }

    @Override
    public String name() {
        return settings.getName();
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
                        .withPath(zkNode)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, JdbcConnectionSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("JDBC Connection settings not found. [path=%s]", zkPath));
                }
                settings = (JdbcConnectionSettings) reader.settings();
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
                this.settings = (JdbcConnectionSettings) settings;
                this.connectionManager = env.connectionManager();
                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    protected String createJdbcUrl(KeyStore keyStore) throws Exception {
        String url = settings.getJdbcUrl().trim();
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        StringBuilder builder = new StringBuilder(url);
        if (!Strings.isNullOrEmpty(settings.getDb())) {
            builder.append("/")
                    .append(settings.getDb());
        }
        builder.append("?")
                .append(Constants.DB_KEY_USER)
                .append(settings.getUser());
        String pk = settings.getPassword();
        builder.append("&")
                .append(Constants.DB_KEY_PASSWD)
                .append(keyStore.read(pk));
        builder.append("&")
                .append(Constants.DB_KEY_POOL_SIZE)
                .append(settings.getPoolSize());
        if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
            for (String param : settings.getParameters().keySet()) {
                builder.append("&")
                        .append(param)
                        .append("=")
                        .append(settings.getParameters().get(param));
            }
        }
        return builder.toString();
    }

    public abstract java.sql.Connection getConnection() throws SQLException;

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
        return settings.getType();
    }
}
