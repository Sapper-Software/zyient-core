package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.db.JdbcConnection;
import ai.sapper.cdc.core.model.Encrypted;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;


/**
 * <pre>
 *     <connections>
 *         <connection>
 *             <class>[Connection class]</class>
 *             <jdbc>
 *                  <name>[Connection name, must be unique]</name>
 *                  <driver>[JDBC Driver class]</driver>
 *                  <dialect>[JDBC Dialect (optional)</dialect>
 *                  <jdbcUrl>[JDBC Connection URL]</jdbcUrl>
 *                  <user>[DB User name]</user>
 *                  <passwordKey>[Password Key in the KeyStore]</passwordKey>
 *                  <poolSize>[Connection Pool Size, default = 32]</poolSize>
 *                  <db>[Database name (optional)]</db>
 *             </jdbc>
 *         </connection>
 *     </connections>
 *     ...
 *     <save>[Save connections to ZooKeeper, default=false]</save>
 *     <override>[Override saved connections, default = true]</override>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class JdbcConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_DRIVER = "driver";
        public static final String CONFIG_DIALECT = "dialect";
        public static final String CONFIG_JDBC_URL = "jdbcUrl";
        public static final String CONFIG_USER = "user";
        public static final String CONFIG_PASS_KEY = "passwordKey";
        public static final String CONFIG_POOL_SIZE = "poolSize";
        public static final String CONFIG_DB_NAME = "db";
    }

    @Config(name = Constants.CONFIG_DRIVER)
    private String jdbcDriver;
    @Config(name = Constants.CONFIG_DIALECT, required = false)
    private String jdbcDialect;
    @Config(name = Constants.CONFIG_JDBC_URL)
    private String jdbcUrl;
    @Config(name = Constants.CONFIG_DB_NAME, required = false)
    private String db;
    @Config(name = Constants.CONFIG_USER)
    private String user;
    @Encrypted
    @Config(name = Constants.CONFIG_PASS_KEY)
    private String password;
    @Config(name = Constants.CONFIG_POOL_SIZE, required = false, type = Integer.class)
    private int poolSize = 32;

    public JdbcConnectionSettings() {
        setType(EConnectionType.db);
    }

    public JdbcConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof JdbcConnectionSettings);
        jdbcDriver = ((JdbcConnectionSettings) settings).jdbcDriver;
        jdbcDialect = ((JdbcConnectionSettings) settings).jdbcDialect;
        db = ((JdbcConnectionSettings) settings).db;
        user = ((JdbcConnectionSettings) settings).user;
        password = ((JdbcConnectionSettings) settings).password;
        poolSize = ((JdbcConnectionSettings) settings).poolSize;
    }
}
