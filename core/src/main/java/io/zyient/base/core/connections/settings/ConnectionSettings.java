/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.core.connections.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.model.ESettingsSource;
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

    public void validate() throws Exception {
        Field[] fields = ReflectionHelper.getAllFields(getClass());
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Config.class)) {
                    Config c = field.getAnnotation(Config.class);
                    if (c.required()) {
                        Object v = ReflectionHelper.reflectionUtils().getFieldValue(this, field);
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
