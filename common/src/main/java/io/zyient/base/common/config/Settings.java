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
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
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

    private boolean hasParameter(@NonNull String key) {
        if (parameters != null) {
            return parameters.containsKey(key);
        }
        return false;
    }

    public String getParameter(@NonNull String key) {
        if (hasParameter(key)) {
            return parameters.get(key);
        }
        return null;
    }

    public Map<String, String> serialize() throws Exception {
        Map<String, String> values = new HashMap<>();
        Field[] fields = ReflectionHelper.getAllFields(getClass());
        if (fields == null || fields.length == 0) {
            return null;
        }
        values.put(CONFIG_SETTINGS_TYPE, getClass().getCanonicalName());
        for (Field field : fields) {
            if (field.isAnnotationPresent(Config.class)) {
                Config config = field.getAnnotation(Config.class);
                Object v = ReflectionHelper.reflectionUtils().getFieldValue(this, field);
                if (v == null) {
                    if (config.required()) {
                        throw new ConfigurationException(
                                String.format("Missing field value. [field=%s]", field.getName()));
                    }
                } else {
                    if (!ReflectionHelper.isPrimitiveTypeOrString(config.type())) {
                        if (config.type().equals(Class.class)) {
                            Class<?> t = (Class<?>) v;
                            values.put(config.name(), t.getCanonicalName());
                        } else {
                            String json = JSONUtils.asString(v);
                            values.put(config.name(), json);
                        }
                    } else
                        values.put(config.name(), String.valueOf(v));
                }
            }
        }
        return values;
    }
}
