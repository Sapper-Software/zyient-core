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

package io.zyient.core.mapping.transformers;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.core.mapping.DataException;
import io.zyient.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class EnumTransformer<T extends Enum<T>> extends DeSerializer<T> {
    public static final String __CONFIG_PATH_ENUMS = "enums";

    private Class<T> type;
    private Map<String, String> enumValues;

    public EnumTransformer(@NonNull Class<T> type) {
        super(type);
        this.type = type;
    }

    @Override
    public DeSerializer<T> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        try {
            name = type.getSimpleName();
            if (enumValues != null) {
                for (String key : enumValues.keySet()) {
                    Enum<T> e = Enum.valueOf(type, enumValues.get(key));
                }
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public DeSerializer<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            name = type.getSimpleName();
            if (ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH_ENUMS)) {
                enumValues = ConfigReader.readAsMap(xmlConfig, __CONFIG_PATH_ENUMS);
            }
            if (enumValues != null) {
                for (String key : enumValues.keySet()) {
                    Enum<T> e = Enum.valueOf(type, enumValues.get(key));
                }
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T transform(@NonNull Object source) throws DataException {
        if (source.getClass().isEnum()) {
            return (T) source;
        } else if (source instanceof String value) {
            if (enumValues.containsKey(value)) {
                value = enumValues.get(value);
            }
            return Enum.valueOf(type, value);
        } else if (source instanceof Integer iv) {
            return type.getEnumConstants()[iv];
        }
        throw new DataException(String.format("Cannot transform to Enum. [source=%s]", source.getClass()));
    }

    @Override
    public String serialize(@NonNull T value) throws DataException {
        return value.name();
    }
}
