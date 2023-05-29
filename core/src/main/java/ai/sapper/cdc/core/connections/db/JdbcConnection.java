package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.settngs.JdbcConnectionSettings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.sql.SQLException;

@Getter
@Accessors(fluent = true)
public class JdbcConnection extends DbConnection {

    private ComboPooledDataSource poolDataSource;
    private JdbcConnectionConfig config;

    public JdbcConnection() {
        super(JdbcConnectionConfig.__CONFIG_PATH);
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
                config = new JdbcConnectionConfig(xmlConfig);
                config.read();
                settings = (JdbcConnectionSettings) config.settings();

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
                String jdbc = createJdbcUrl(keyStore);
                poolDataSource = new ComboPooledDataSource();
                poolDataSource.setJdbcUrl(jdbc);
                poolDataSource.setDriverClass(settings.getJdbcDriver());
                poolDataSource.setInitialPoolSize(8);

                state.setState(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }


    @Override
    public String path() {
        return JdbcConnectionConfig.__CONFIG_PATH;
    }

    public java.sql.Connection getConnection() throws SQLException {
        Preconditions.checkState(isConnected());
        return poolDataSource.getConnection();
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.setState(EConnectionState.Closed);
            }
            if (poolDataSource != null) {
                poolDataSource.close();
                poolDataSource = null;
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class JdbcConnectionConfig extends DbConnectionConfig {
        public static final String __CONFIG_PATH = "jdbc";

        public JdbcConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, JdbcConnection.class);
        }
    }
}
