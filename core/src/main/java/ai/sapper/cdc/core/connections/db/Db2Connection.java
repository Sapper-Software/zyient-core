package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settngs.JdbcConnectionSettings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class Db2Connection extends DbConnection {
    public static final String TYPE_DB2_Z = "DB2/z";
    public static final String TYPE_DB2_LUW = "DB2/LUW";

    private Db2ConnectionConfig config;
    private Properties connectionProps;

    public Db2Connection() {
        super(Db2ConnectionConfig.__CONFIG_PATH);
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
                config = new Db2ConnectionConfig(xmlConfig);
                config.read();

                settings = (JdbcConnectionSettings) config.settings();

                state.state(EConnectionState.Initialized);
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
                        .withPath(zkNode)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, Db2ConnectionSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("JDBC Connection settings not found. [path=%s]", zkPath));
                }
                settings = (JdbcConnectionSettings) reader.settings();
                settings.validate();

                this.connectionManager = env.connectionManager();
                state.state(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    public String db2Type() {
        return ((Db2ConnectionSettings) config.settings()).db2Type;
    }

    protected String createJdbcUrl() throws Exception {
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
        return builder.toString();
    }

    @Override
    public Connection connect() throws ConnectionError {
        KeyStore keyStore = connectionManager().keyStore();
        Preconditions.checkNotNull(keyStore);
        synchronized (state) {
            if (state.isConnected()) return this;
            Preconditions.checkState(state.state() == EConnectionState.Initialized);
            try {
                String pk = settings.getPassword();

                connectionProps = new Properties();
                String username = settings.getUser();
                String password = keyStore.read(pk);
                if (username != null) {
                    connectionProps.setProperty("user", username);
                }
                if (password != null) {
                    connectionProps.setProperty("password", password);
                }
                if (settings.getParameters() != null) {
                    connectionProps.putAll(settings.getParameters());
                }

                state.state(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    public java.sql.Connection getConnection() throws SQLException {
        Preconditions.checkState(isConnected());
        KeyStore keyStore = connectionManager().keyStore();
        Preconditions.checkNotNull(keyStore);

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            return DriverManager.getConnection(
                    createJdbcUrl(),
                    connectionProps);
        } catch (ClassNotFoundException ex) {
            throw new SQLException("No sql driver found.");
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
        }
    }


    @Override
    public String path() {
        return Db2ConnectionConfig.__CONFIG_PATH;
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class Db2ConnectionSettings extends JdbcConnectionSettings {
        @Config(name = "db2type", required = false)
        private String db2Type = TYPE_DB2_LUW;
    }

    @Getter
    @Accessors(fluent = true)
    public static class Db2ConnectionConfig extends DbConnectionConfig {
        public static final String __CONFIG_PATH = "db2";


        public Db2ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, Db2ConnectionSettings.class, Db2Connection.class);
        }
    }
}
