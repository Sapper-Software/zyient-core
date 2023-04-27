package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.connections.Connection;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public abstract class ConnectionSettings extends Settings {
    public static final String CONFIG_CLASS = "@class";

    @Config(name = "type", type = EConnectionType.class)
    private EConnectionType type;
    @Config(name = "source", type = ESettingsSource.class)
    private ESettingsSource source;
    @Config(name = "name")
    private String name;
    @Config(name = "class", type = Class.class)
    private Class<? extends Connection> connectionClass;

    public ConnectionSettings() {
        source = ESettingsSource.File;
    }

    public ConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        type = settings.type;
        source = settings.source;
        name = settings.name;
        connectionClass = settings.connectionClass;
    }

    public abstract void validate() throws Exception;
}
