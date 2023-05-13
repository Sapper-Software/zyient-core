package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.connections.Connection;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Field;

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

    public final void validate() throws Exception {
        Field[] fields = ReflectionUtils.getAllFields(getClass());
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Config.class)) {
                    Config c = field.getAnnotation(Config.class);
                    if (c.required()) {
                        Object v = ReflectionUtils.getFieldValue(this, field);
                        if (v == null) {
                            throw new Exception(String.format("[%s] Missing required field. [field=%s]",
                                    getClass().getCanonicalName(), c.name()));
                        }
                    }
                }
            }
        }
    }
}
