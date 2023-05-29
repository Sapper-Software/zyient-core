package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.JdbcConnectionSettings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Strings;
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
