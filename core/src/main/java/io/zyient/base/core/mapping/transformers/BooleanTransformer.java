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

import com.google.common.base.Strings;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.mapping.DataException;
import io.zyient.base.core.mapping.mapper.MappingSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class BooleanTransformer implements Transformer<Boolean> {
    @Override
    public String name() {
        return Boolean.class.getCanonicalName();
    }

    @Override
    public Transformer<Boolean> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        return this;
    }

    @Override
    public Boolean transform(@NonNull Object source) throws DataException {
        if (ReflectionHelper.isBoolean(source.getClass())) {
            return (boolean) source;
        } else if (ReflectionHelper.isNumericType(source.getClass())) {
            int v = (int) source;
            return (v > 0);
        } else if (source instanceof String value) {
            if (Strings.isNullOrEmpty(value)) {
                return null;
            }
            return Boolean.parseBoolean(value);
        }
        throw new DataException(String.format("Cannot transform to Boolean. [source=%s]", source.getClass()));
    }

    @Override
    public String write(@NonNull Boolean source) throws DataException {
        return source.toString();
    }
}
