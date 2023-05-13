package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.JdbcConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class DbConnectionConfig extends ConnectionConfig {
    private final Class<? extends DbConnection> connectionClass;

    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path,
                              @NonNull Class<? extends DbConnection> connectionClass) {
        super(config, path, JdbcConnectionSettings.class);
        this.connectionClass = connectionClass;
    }

    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path,
                              @NonNull Class<? extends ConnectionSettings> type,
                              @NonNull Class<? extends DbConnection> connectionClass) {
        super(config, path, type);
        this.connectionClass = connectionClass;
    }

    @Override
    public void read() throws ConfigurationException {
        super.read();
        JdbcConnectionSettings settings = (JdbcConnectionSettings) settings();
        settings.setConnectionClass(connectionClass);
    }
}
