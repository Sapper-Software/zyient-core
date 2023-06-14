package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
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
public abstract class ConnectionConfig extends ConfigReader {
    private final Class<? extends Connection> connectionClass;

    public ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull String path,
                            @NonNull Class<? extends ConnectionSettings> type,
                            @NonNull Class<? extends Connection> connectionClass) {
        super(config, path, type);
        this.connectionClass = connectionClass;
    }

    public ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull String path,
                            @NonNull Settings settings,
                            @NonNull Class<? extends Connection> connectionClass) {
        super(config, path, settings);
        this.connectionClass = connectionClass;
    }

    @Override
    public void read() throws ConfigurationException {
        super.read();
        ConnectionSettings settings = (ConnectionSettings) settings();
        settings.withConnectionClass(connectionClass);
    }
}
