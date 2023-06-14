package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.settings.ConnectionSettings;
import ai.sapper.cdc.core.connections.settings.JdbcConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class DbConnectionConfig extends ConnectionConfig {
    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path,
                              @NonNull Class<? extends DbConnection> connectionClass) {
        super(config, path, JdbcConnectionSettings.class, connectionClass);
    }

    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path,
                              @NonNull Class<? extends ConnectionSettings> type,
                              @NonNull Class<? extends DbConnection> connectionClass) {
        super(config, path, type, connectionClass);
    }

}
