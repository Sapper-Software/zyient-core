package ai.sapper.cdc.entity.manager;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SchemaDataHandlerSettings extends Settings {
    public static final String __CONFIG_PATH = "persistence";

    @Config(name = "connection")
    private String connection;
    @Config(name = "source")
    private String source;
}
