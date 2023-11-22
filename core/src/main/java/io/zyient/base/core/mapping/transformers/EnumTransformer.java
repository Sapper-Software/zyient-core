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

package io.zyient.base.core.mapping.transformers;

import com.google.common.base.Preconditions;
import io.zyient.base.core.mapping.DataException;
import io.zyient.base.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class EnumTransformer implements Transformer<Enum<?>> {
    private Class<? extends Enum> type;
    private Map<String, String> enumValues;

    @Override
    public String name() {
        Preconditions.checkNotNull(type);
        return type.getSimpleName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Transformer<Enum<?>> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        try {
            if (enumValues != null) {
                for (String key : enumValues.keySet()) {
                    Enum<?> e = Enum.valueOf(type, enumValues.get(key));
                }
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Enum<?> transform(@NonNull Object source) throws DataException {
        if (source.getClass().isEnum()) {
            return (Enum<?>) source;
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
    public String write(@NonNull Enum<?> source) throws DataException {
        String value = source.name();
        if (enumValues != null) {
            for (String key : enumValues.keySet()) {
                String v = enumValues.get(key);
                if (v.equals(value)) {
                    value = key;
                    break;
                }
            }
        }
        return value;
    }
}
