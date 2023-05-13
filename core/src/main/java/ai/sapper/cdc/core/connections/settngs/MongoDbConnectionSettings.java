package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.connections.db.MongoDbConnection;
import ai.sapper.cdc.core.model.Encrypted;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MongoDbConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_HOST = "host";
        public static final String CONFIG_PORT = "port";
        public static final String CONFIG_DB = "db";
    }

    @Config(name = JdbcConnectionSettings.Constants.CONFIG_USER)
    private String user;
    @Config(name = Constants.CONFIG_HOST)
    private String host;
    @Config(name = Constants.CONFIG_PORT, required = false, type = Integer.class)
    private int port = 27017;
    @Config(name = Constants.CONFIG_DB)
    private String db;
    @Encrypted
    @Config(name = JdbcConnectionSettings.Constants.CONFIG_PASS_KEY)
    private String password;
    @Config(name = JdbcConnectionSettings.Constants.CONFIG_POOL_SIZE, required = false, type = Integer.class)
    private int poolSize = 32;

    public MongoDbConnectionSettings() {
        setConnectionClass(MongoDbConnection.class);
        setType(EConnectionType.db);
    }

    public MongoDbConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof MongoDbConnectionSettings);
        this.user = ((MongoDbConnectionSettings) settings).getUser();
        this.host = ((MongoDbConnectionSettings) settings).getHost();
        this.port = ((MongoDbConnectionSettings) settings).getPort();
        this.db = ((MongoDbConnectionSettings) settings).getDb();
        this.password = ((MongoDbConnectionSettings) settings).password;
        this.poolSize = ((MongoDbConnectionSettings) settings).getPoolSize();
    }
}
