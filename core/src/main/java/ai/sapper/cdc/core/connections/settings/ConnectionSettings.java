package ai.sapper.cdc.core.connections.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.model.ESettingsSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Field;

/**
 * <pre>
 *     <connections>
 *         <shared>
 *              <connection>[ZooKeeper connection name]</connection>
 *              <path>[Connections registry path]</path>
 *         </shared>
 *         <connection>
 *             <class>[Connection class]</class>
 *             <[type]>
 *                 <name>[Connection name, must be unique]</name>
 *                 ...
 *             </[type]>
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
public abstract class ConnectionSettings extends Settings {
    private EConnectionType type;
    @JsonIgnore
    private ESettingsSource source;
    @Config(name = "name")
    private String name;
    private Class<? extends Connection> connectionClass;

    public ConnectionSettings() {
        source = ESettingsSource.File;
    }

    public ConnectionSettings withConnectionClass(@NonNull Class<? extends Connection> connectionClass) {
        this.connectionClass = connectionClass;
        return this;
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