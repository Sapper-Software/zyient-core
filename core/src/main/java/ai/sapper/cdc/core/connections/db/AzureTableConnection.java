package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.*;
import ai.sapper.cdc.core.connections.settngs.AzureTableConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.google.common.base.Preconditions;
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
                state.clear(EConnectionState.Unknown);
                this.connectionManager = env.connectionManager();
                config = new AzureTableConnectionConfig(xmlConfig);
                config.read();
                settings = (AzureTableConnectionSettings) config.settings();

                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
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
                state.clear(EConnectionState.Unknown);
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
                state.clear(EConnectionState.Unknown);
                this.settings = (AzureTableConnectionSettings) settings;
                this.connectionManager = env.connectionManager();
                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
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
            super(config, __CONFIG_PATH, AzureTableConnectionSettings.class);
        }
    }
}
