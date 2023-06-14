package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.connections.db.AzureTableConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AzureTableConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_CONNECTION_STRING = "connectionString";
        public static final String CONFIG_DB_NAME = "db";
    }

    @Config(name = Constants.CONFIG_CONNECTION_STRING)
    private String connectionString;
    @Config(name = Constants.CONFIG_DB_NAME)
    private String db;

    public AzureTableConnectionSettings() {
        setType(EConnectionType.db);
    }

    public AzureTableConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof AzureTableConnectionSettings);
        connectionString = ((AzureTableConnectionSettings) settings).getConnectionString();
        db = ((AzureTableConnectionSettings) settings).db;
    }
}
