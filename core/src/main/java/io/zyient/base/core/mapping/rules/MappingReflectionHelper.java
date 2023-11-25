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

package io.zyient.base.core.mapping.rules;

import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.model.MappedResponse;
import lombok.NonNull;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingReflectionHelper {
    public static final String FIELD_ENTITY = "entity";
    public static final String FIELD_CUSTOM = "entity.property";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_CACHED = "cached";
    public static final String FIELD_REGEX = "(\\$\\{(.*?)\\})";

    public static Map<String, String> extractFields(@NonNull String query) {
        Pattern fieldFinder = Pattern.compile(FIELD_REGEX);
        Matcher m = fieldFinder.matcher(query);
        Map<String, String> fields = new HashMap<>();
        while (m.find()) {
            String exp = m.group(1);
            String var = m.group(2);
            if (var.startsWith("custom.")) {
                var = var.replace("custom\\.", FIELD_CUSTOM);
            } else if (prefixEntityField(var)) {
                var = FIELD_ENTITY + "." + var;
            }
            fields.put(exp, var);
        }
        return fields;
    }

    public static Field findField(@NonNull String name, @NonNull Class<?> entityType) {
        if (name.startsWith(FIELD_ENTITY)) {
            name = removePrefix(name, FIELD_ENTITY);
            return ReflectionUtils.findField(entityType, name);
        }
        return ReflectionUtils.findField(MappedResponse.class, name);
    }

    private static String removePrefix(String name, String prefix) {
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }
        return name.replaceFirst(prefix, "");
    }

    private static boolean prefixEntityField(String field) {
        return !field.startsWith(FIELD_SOURCE) &&
                !field.startsWith(FIELD_ENTITY) &&
                !field.startsWith(FIELD_CACHED);
    }
}
