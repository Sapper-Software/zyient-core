package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settings.ConnectionSettings;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import ai.sapper.cdc.core.connections.settings.MongoDbConnectionSettings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class MongoDbConnection implements Connection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    protected ConnectionManager connectionManager;
    protected MongoDbConnectionSettings settings;
    private MongoClient client;

    protected final String zkNode;
    private MongoDbConnectionConfig config;

    public MongoDbConnection() {
        this.zkNode = MongoDbConnectionConfig.Constants.__CONFIG_PATH;
    }

    @Override
    public String name() {
        return settings.getName();
    }

    public MongoClient client() {
        Preconditions.checkState(state.isConnected());
        return client;
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
                config = new MongoDbConnectionConfig(xmlConfig);
                config.read();
                settings = (MongoDbConnectionSettings) config.settings();

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
                        .withPath(zkNode)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, MongoDbConnectionSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("MongoDB Connection settings not found. [path=%s]", zkPath));
                }
                settings = (MongoDbConnectionSettings) reader.settings();
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
                this.settings = (MongoDbConnectionSettings) settings;
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
        KeyStore keyStore = connectionManager().keyStore();
        Preconditions.checkNotNull(keyStore);
        synchronized (state) {
            if (state.isConnected()) return this;
            Preconditions.checkState(state.getState() == EConnectionState.Initialized);
            try {
                String url = createConnectionUrl(keyStore);
                client = MongoClients.create(url);
                state.setState(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    protected String createConnectionUrl(KeyStore keyStore) throws Exception {
        String url = "mongodb://";
        StringBuilder builder = new StringBuilder(url);
        builder.append(settings.getUser());
        String pk = settings.getPassword();
        builder.append(":")
                .append(keyStore.read(pk));
        builder.append("@")
                .append(settings.getHost())
                .append(":")
                .append(settings.getPort());
        builder.append("/")
                .append(settings.getDb());
        builder.append("/?")
                .append(DbConnection.Constants.DB_KEY_POOL_SIZE)
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
        return EConnectionType.db;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            synchronized (state) {
                if (client != null) {
                    client.close();
                    client = null;
                }
            }
        }
        state.setState(EConnectionState.Closed);
    }
}
