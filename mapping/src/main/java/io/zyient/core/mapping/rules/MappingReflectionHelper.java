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

package io.zyient.core.mapping.rules;

import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.model.PropertyBag;
import io.zyient.core.mapping.model.ExtendedPropertyModel;
import io.zyient.core.mapping.model.MappedResponse;
import lombok.NonNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingReflectionHelper {
    public static final String FIELD_ENTITY = "entity";
    public static final String FIELD_PROPERTY = "property";
    public static final String FIELD_CUSTOM = String.format("entity.%s", FIELD_PROPERTY);
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_CACHED = "cached";
    public static final String FIELD_REGEX = "(\\$\\{(.*?)\\})";
    public static final String KEY_REGEX = "\\['?(.*?)'?\\]";
    public static final String METHOD_SET_PROPERTY = "setProperty";
    public static final String METHOD_GET_PROPERTY = "getProperty";
    private static final Pattern PATTERN_FIND_FIELD = Pattern.compile(FIELD_REGEX);
    private static final Pattern PATTERN_KEY = Pattern.compile(KEY_REGEX);


    public static Map<String, String> extractFields(@NonNull String query) {
        Matcher m = PATTERN_FIND_FIELD.matcher(query);
        Map<String, String> fields = new HashMap<>();
        while (m.find()) {
            String exp = m.group(1);
            String var = m.group(2);
            if (prefixEntityField(var)) {
                var = FIELD_ENTITY + "." + var;
            }
            fields.put(exp, var);
        }
        return fields;
    }

    public static List<String> extractKey(@NonNull String name) {
        List<String> keys = new ArrayList<>();
        Matcher m = PATTERN_KEY.matcher(name);
        while (m.find()) {
            keys.add(m.group(1));
        }
        if (!keys.isEmpty()) {
            return keys;
        }
        return null;
    }

    public static PropertyModel findField(@NonNull String name,
                                          @NonNull Class<?> entityType) throws Exception {
        if (isCachePrefixed(name)) {
            return ReflectionHelper.findProperty(MappedResponse.class, FIELD_CACHED);
        } else if (isSourcePrefixed(name)) {
            return ReflectionHelper.findProperty(MappedResponse.class, FIELD_SOURCE);
        } else if (isPropertyPrefixed(name)) {
            if (!ReflectionHelper.implementsInterface(PropertyBag.class, entityType)) {
                throw new Exception(String.format("Cannot set custom property for type. [type=%s]",
                        entityType.getCanonicalName()));
            }
            ExtendedPropertyModel pm = new ExtendedPropertyModel();
            pm.property(name);
            List<String> keys = extractKey(name);
            if (keys == null) {
                throw new Exception(String.format("Failed to extract property key. [name=%s]", name));
            }
            pm.key(keys.get(0));
            List<Method> setters = ReflectionHelper.findMethod(entityType,
                    METHOD_SET_PROPERTY,
                    false);
            if (setters != null) {
                for (Method m : setters) {
                    Class<?>[] params = m.getParameterTypes();
                    if (params.length == 2) {
                        if (params[0].equals(String.class)) {
                            if (params[1].equals(Object.class)) {
                                pm.setter(m);
                                break;
                            }
                        }
                    }
                }
            }
            List<Method> getters = ReflectionHelper.findMethod(entityType,
                    METHOD_GET_PROPERTY,
                    false);
            if (getters != null) {
                for (Method m : getters) {
                    Class<?>[] params = m.getParameterTypes();
                    if (params.length == 1) {
                        if (params[0].equals(String.class)) {
                            pm.getter(m);
                            break;
                        }
                    }
                }
            }
            if (pm.getter() == null || pm.setter() == null) {
                throw new Exception(String.format("Property Getter/Setter method not found. [type=%s]",
                        entityType.getCanonicalName()));
            }
            return pm;
        } else {
            if (isEntityPrefixed(name)) {
                name = removePrefix(name, FIELD_ENTITY);
            }
            return ReflectionHelper.findProperty(entityType, name);
        }
    }

    public static String normalizeField(@NonNull String field) throws Exception {
        if (isCachePrefixed(field) || isSourcePrefixed(field)) {
            return field;
        } else if (!isEntityPrefixed(field)) {
            return dot(FIELD_ENTITY) + field;
        }
        return field;
    }

    public static String fieldToPropertySetMethod(@NonNull String field) throws Exception {
        if (isPropertyPrefixed(field) || isEntityPropertyPrefixed(field)) {
            List<String> keys = extractKey(field);
            if (keys != null && !keys.isEmpty()) {
                if (keys.size() > 1) {
                    throw new Exception(String.format("Invalid property: [name=%s]", field));
                }
                String key = keys.get(0);
                return String.format("%s.%s(\"%s\")", FIELD_ENTITY, METHOD_SET_PROPERTY, key);
            } else {
                throw new Exception(String.format("Invalid property: [name=%s]", field));
            }
        }
        return null;
    }

    public static String fieldToPropertyGetMethod(@NonNull String field) throws Exception {
        if (isPropertyPrefixed(field) || isEntityPropertyPrefixed(field)) {
            List<String> keys = extractKey(field);
            if (keys != null && !keys.isEmpty()) {
                if (keys.size() > 1) {
                    throw new Exception(String.format("Invalid property: [name=%s]", field));
                }
                String key = keys.get(0);
                return String.format("%s.%s(\"%s\")", FIELD_ENTITY, METHOD_GET_PROPERTY, key);
            } else {
                throw new Exception(String.format("Invalid property: [name=%s]", field));
            }
        }
        return null;
    }

    public static void setProperty(@NonNull String field,
                                   @NonNull PropertyModel property,
                                   @NonNull Object entity,
                                   Object value) throws Exception {
        if (property instanceof ExtendedPropertyModel) {
            PropertyBag pb = (PropertyBag) entity;
            pb.setProperty(((ExtendedPropertyModel) property).key(), value);
        } else {
            ReflectionHelper.setFieldValue(value, entity, field);
        }
    }

    public static Object getProperty(@NonNull String field,
                                     @NonNull PropertyModel property,
                                     @NonNull Object entity) throws Exception {
        if (property instanceof ExtendedPropertyModel) {
            PropertyBag pb = (PropertyBag) entity;
            return pb.getProperty(((ExtendedPropertyModel) property).key());
        }
        if (isSourcePrefixed(field)) {
            if (entity instanceof MappedResponse<?>) {
                return getSourceProperty(field, ((MappedResponse<?>) entity).source());
            } else {
                throw new Exception(String.format("Source fields not present. [type=%s]",
                        entity.getClass().getCanonicalName()));
            }
        }
        return ReflectionHelper.getFieldValue(entity, field);
    }

    @SuppressWarnings("unchecked")
    public static Object getSourceProperty(@NonNull String field,
                                           Map<String, Object> source) {
        List<String> keys = extractKey(field);
        if (keys != null && !keys.isEmpty()) {
            Map<String, Object> node = source;
            for (int ii = 0; ii < keys.size(); ii++) {
                String key = keys.get(ii);
                if (ii == keys.size() - 1) {
                    return node.get(key);
                }
                Object value = node.get(key);
                if (value instanceof Map<?, ?>) {
                    node = (Map<String, Object>) value;
                }
            }
        }
        return null;
    }

    public static String dot(String name) {
        if (!name.endsWith(".")) {
            name = name + ".";
        }
        return name;
    }

    public static String removePrefix(String name, String prefix) {
        prefix = dot(prefix);
        if (name.startsWith(prefix))
            return name.replaceFirst(prefix, "");
        return name;
    }

    public static boolean isCachePrefixed(@NonNull String name) {
        return name.startsWith(FIELD_CACHED + "[");
    }

    public static boolean isEntityPropertyPrefixed(@NonNull String name) {
        return name.startsWith(FIELD_CUSTOM + "[");
    }

    public static boolean isPropertyPrefixed(@NonNull String name) {
        return name.startsWith(FIELD_PROPERTY + "[");
    }

    public static boolean isSourcePrefixed(@NonNull String name) {
        return name.startsWith(FIELD_SOURCE + "[");
    }

    public static boolean isEntityPrefixed(@NonNull String name) {
        return name.startsWith(FIELD_ENTITY + ".");
    }

    private static boolean prefixEntityField(String field) {
        return !isSourcePrefixed(field)
                && !isCachePrefixed(field)
                && !isEntityPrefixed(field);
    }
}