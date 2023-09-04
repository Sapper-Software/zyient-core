/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.common.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class Settings {
    public static final String CONFIG_SETTINGS_TYPE = "@class";
    public static final String CONFIG_PARAMS = "parameters";

    @Config(name = CONFIG_PARAMS, required = false, type = Map.class)
    private Map<String, String> parameters;

    public Settings() {

    }

    public Settings(@NonNull Settings source) {
        this.parameters = source.parameters;
    }


    public Map<String, String> serialize() throws Exception {
        Map<String, String> values = new HashMap<>();
        Field[] fields = ReflectionUtils.getAllFields(getClass());
        if (fields == null || fields.length == 0) {
            return null;
        }
        values.put(CONFIG_SETTINGS_TYPE, getClass().getCanonicalName());
        for (Field field : fields) {
            if (field.isAnnotationPresent(Config.class)) {
                Config config = field.getAnnotation(Config.class);
                Object v = ReflectionUtils.getFieldValue(this, field);
                if (v == null) {
                    if (config.required()) {
                        throw new ConfigurationException(
                                String.format("Missing field value. [field=%s]", field.getName()));
                    }
                } else {
                    if (!ReflectionUtils.isPrimitiveTypeOrString(config.type())) {
                        if (config.type().equals(Class.class)) {
                            Class<?> t = (Class<?>) v;
                            values.put(config.name(), t.getCanonicalName());
                        } else {
                            String json = JSONUtils.asString(v, v.getClass());
                            values.put(config.name(), json);
                        }
                    } else
                        values.put(config.name(), String.valueOf(v));
                }
            }
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Settings> T read(@NonNull Map<String, String> values) throws Exception {
        String type = values.get(CONFIG_SETTINGS_TYPE);
        if (Strings.isNullOrEmpty(type)) {
            throw new ConfigurationException(
                    String.format("Settings type not defined. [key=%s]", CONFIG_SETTINGS_TYPE));
        }
        Class<T> cls = (Class<T>) Class.forName(type);
        T settings = cls.getDeclaredConstructor().newInstance();
        Field[] fields = ReflectionUtils.getAllFields(cls);
        if (fields != null && fields.length > 0) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Config.class)) {
                    Config config = field.getAnnotation(Config.class);
                    String v = values.get(config.name());
                    if (config.type().equals(Exists.class)) {
                        if (Strings.isNullOrEmpty(v)) {
                            ReflectionUtils.setBooleanValue(settings, field, false);
                        } else {
                            Boolean b = Boolean.parseBoolean(v);
                            ReflectionUtils.setBooleanValue(settings, field, b);
                        }
                        continue;
                    }
                    if (Strings.isNullOrEmpty(v)) {
                        if (config.required()) {
                            throw new ConfigurationException(
                                    String.format("Missing field value. [field=%s]", field.getName()));
                        }
                    } else {
                        if (!ReflectionUtils.isPrimitiveTypeOrString(config.type())) {
                            if (config.type().equals(Class.class)) {
                                Class<?> t = Class.forName(v);
                                ReflectionUtils.setValue(t, settings, field);
                            } else {
                                Object o = JSONUtils.read(v, config.type());
                                ReflectionUtils.setValue(o, settings, field);
                            }
                        } else {
                            ReflectionUtils.setValueFromString(v, settings, field);
                        }
                    }
                }
            }
        }
        return settings;
    }
}
