package ai.sapper.cdc.core.connections.settings;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Db2ConnectionSettings extends JdbcConnectionSettings {
    public static final String TYPE_DB2_Z = "DB2/z";
    public static final String TYPE_DB2_LUW = "DB2/LUW";


    @Config(name = "db2type", required = false)
    private String db2Type = TYPE_DB2_LUW;
}
