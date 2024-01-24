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

package io.zyient.core.persistence;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigValueParser;
import lombok.NonNull;

import java.util.HashSet;
import java.util.Set;

public class ClassSetParser implements ConfigValueParser<Set<Class<?>>> {
    public static final String SEPARATOR = ";";

    @Override
    public Set<Class<?>> parse(@NonNull String value) throws Exception {
        if (!Strings.isNullOrEmpty(value)) {
            String[] parts = value.split(SEPARATOR);
            if (parts.length > 0) {
                Set<Class<?>> types = new HashSet<>();
                for (String part : parts) {
                    types.add(Class.forName(part));
                }
                return types;
            }
        }
        return null;
    }

    @Override
    public String serialize(@NonNull Set<Class<?>> value) throws Exception {
        StringBuilder builder = new StringBuilder();
        for (Class<?> cls : value) {
            if (!builder.isEmpty()) {
                builder.append(SEPARATOR);
            }
            builder.append(cls.getCanonicalName());
        }
        return builder.toString();
    }
}
