package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.Setting;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class ConnectionConfig extends ConfigReader {

    public ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull String path,
                            @NonNull Class<? extends ConnectionSettings> type) {
        super(config, path, type);
    }
}
