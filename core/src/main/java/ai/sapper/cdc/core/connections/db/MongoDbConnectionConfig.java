package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.settngs.MongoDbConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class MongoDbConnectionConfig extends ConnectionConfig {
    public static class Constants {
        public static final String __CONFIG_PATH = "mongodb";
    }

    public MongoDbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
        super(config,
                Constants.__CONFIG_PATH,
                MongoDbConnectionSettings.class,
                MongoDbConnection.class);
    }
}
