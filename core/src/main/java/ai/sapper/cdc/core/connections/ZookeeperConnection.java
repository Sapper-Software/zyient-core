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

package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.settings.ConnectionSettings;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import ai.sapper.cdc.core.connections.settings.ZookeeperSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class ZookeeperConnection implements Connection {
    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    private CuratorFramework client;
    private CuratorFrameworkFactory.Builder builder;
    private ZookeeperConfig config;
    private ZookeeperSettings settings;

    /**
     * @return
     */
    @Override
    public String name() {
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
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();

                config = new ZookeeperConfig(xmlConfig);
                config.read();
                settings = (ZookeeperSettings) config.settings();

                setup();

                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    private void setup() throws Exception {
        builder = CuratorFrameworkFactory.builder();
        builder.connectString(settings.getConnectionString());
        if (settings.isRetryEnabled()) {
            builder.retryPolicy(new ExponentialBackoffRetry(settings.getRetryInterval(), settings.getRetryCount()));
        }
        if (!Strings.isNullOrEmpty(settings.getAuthenticationHandler())) {
            Class<? extends ZookeeperAuthHandler> cls
                    = (Class<? extends ZookeeperAuthHandler>) Class.forName(settings.getAuthenticationHandler());
            ZookeeperAuthHandler authHandler = cls.getDeclaredConstructor().newInstance();
            authHandler.setup(builder, config.config());
        }
        if (!Strings.isNullOrEmpty(settings.getNamespace())) {
            builder.namespace(settings.getNamespace());
        }
        if (settings.getConnectionTimeout() > 0) {
            builder.connectionTimeoutMs(settings.getConnectionTimeout());
        }
        if (settings.getSessionTimeout() > 0) {
            builder.sessionTimeoutMs(settings.getSessionTimeout());
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
                        .withPath(ZookeeperConfig.__CONFIG_PATH)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, ZookeeperSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("ZooKeeper Connection settings not found. [path=%s]", zkPath));
                }
                settings = (ZookeeperSettings) reader.settings();
                settings.validate();
                setup();

                state.setState(EConnectionState.Initialized);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof ZookeeperSettings);
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                this.settings = (ZookeeperSettings) settings;
                setup();

                state.setState(EConnectionState.Initialized);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        synchronized (state) {
            if (!state.isConnected()
                    && (state.getState() == EConnectionState.Initialized
                    || state.getState() == EConnectionState.Closed)) {
                state.clear();
                try {
                    client = builder.build();
                    client.start();
                    client.blockUntilConnected(15000, TimeUnit.MILLISECONDS);

                    state.setState(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
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
        return state.isConnected();
    }

    @Override
    public String path() {
        return ZookeeperConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.setState(EConnectionState.Closed);
            }
            try {
                if (client != null) {
                    client.close();
                    client = null;
                }
            } catch (Exception ex) {
                state.error(ex);
                throw new IOException("Error closing HDFS connection.", ex);
            }
        }
    }


    public static class ZookeeperConfig extends ConnectionConfig {

        private static final String __CONFIG_PATH = "zookeeper";

        public ZookeeperConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, ZookeeperSettings.class, ZookeeperConnection.class);
        }
    }
}
