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
public class SchemaManagerSettings extends Settings {
    public static final int DEFAULT_SCHEMA_CACHE_SIZE = 128;
    public static final long DEFAULT_CACHE_TIMEOUT = 5 * 60 * 1000;

    @Config(name = "name")
    private String name;
    @Config(name = "persistence.handler.class", type = Class.class)
    private Class<? extends SchemaDataHandler> handler;
    @Config(name = "persistence.handler.settings", type = Class.class)
    private Class<? extends SchemaDataHandlerSettings> handlerSettingsClass;
    @Config(name = "schemaCacheSize", required = false, type = Integer.class)
    private int schemaCacheSize = DEFAULT_SCHEMA_CACHE_SIZE;
    @Config(name = "cacheTimeout", required = false, type = Long.class)
    private long cacheTimeout = DEFAULT_CACHE_TIMEOUT;
    private SchemaDataHandlerSettings handlerSettings;
}
