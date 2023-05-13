package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.ZookeeperSettings;
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
                state.clear(EConnectionState.Unknown);

                config = new ZookeeperConfig(xmlConfig);
                config.read();
                settings = (ZookeeperSettings) config.settings();

                setup();

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    private void setup() throws Exception {
        builder = CuratorFrameworkFactory.builder();
        builder.connectString(settings.getConnectionString());
        if (settings.isRetryEnabled()) {
            builder.retryPolicy(new ExponentialBackoffRetry(settings.getRetryInterval(), settings.getRetryCount()));
        }
        if (!Strings.isNullOrEmpty(settings.getAuthenticationHandler())) {
            Class<? extends ZookeeperAuthHandler> cls
                    = (Class<? extends ZookeeperAuthHandler>) Class.forName(settings.getAuthenticationHandler());
            ZookeeperAuthHandler authHandler = cls.newInstance();
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
                state.clear(EConnectionState.Unknown);

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

                state.state(EConnectionState.Initialized);
            } catch (Exception ex) {
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
                state.clear(EConnectionState.Unknown);
                this.settings = (ZookeeperSettings) settings;
                setup();

                state.state(EConnectionState.Initialized);
            } catch (Exception ex) {
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
                    && (state.state() == EConnectionState.Initialized || state.state() == EConnectionState.Closed)) {
                state.clear(EConnectionState.Initialized);
                try {
                    client = builder.build();
                    client.start();
                    client.blockUntilConnected(15000, TimeUnit.MILLISECONDS);

                    state.state(EConnectionState.Connected);
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
                state.state(EConnectionState.Closed);
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
            super(config, __CONFIG_PATH, ZookeeperSettings.class);
        }
    }
}
