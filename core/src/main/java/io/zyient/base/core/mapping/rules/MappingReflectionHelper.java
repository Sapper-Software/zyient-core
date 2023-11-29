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

import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.mapping.model.ExtendedPropertyModel;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.model.PropertyBag;
import lombok.NonNull;

import java.lang.reflect.Method;
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
    public static final String METHOD_SET_PROPERTY = "setProperty";
    public static final String METHOD_GET_PROPERTY = "getProperty";

    public static Map<String, String> extractFields(@NonNull String query) {
        Pattern fieldFinder = Pattern.compile(FIELD_REGEX);
        Matcher m = fieldFinder.matcher(query);
        Map<String, String> fields = new HashMap<>();
        while (m.find()) {
            String exp = m.group(1);
            String var = m.group(2);
            if (isPropertyPrefixed(var)) {
                var = var.replace("custom\\.", FIELD_CUSTOM);
            } else if (isCachePrefixed(var)) {
                var = removePrefix(var, FIELD_CACHED);
                var = String.format("cached['%s']", var);
            } else if (prefixEntityField(var)) {
                var = FIELD_ENTITY + "." + var;
            }
            fields.put(exp, var);
        }
        return fields;
    }

    public static PropertyModel findField(@NonNull String name,
                                          @NonNull Class<?> entityType) throws Exception {
        if (name.startsWith(dot(FIELD_CACHED))) {
            return ReflectionHelper.findProperty(MappedResponse.class, name);
        } else if (name.startsWith(dot(FIELD_SOURCE))) {
            return ReflectionHelper.findProperty(MappedResponse.class, name);
        } else if (name.startsWith(dot(FIELD_PROPERTY))) {
            if (!ReflectionHelper.implementsInterface(PropertyBag.class, entityType)) {
                throw new Exception(String.format("Cannot set custom property for type. [type=%s]",
                        entityType.getCanonicalName()));
            }
            ExtendedPropertyModel pm = new ExtendedPropertyModel();
            pm.property(name);
            String key = removePrefix(name, FIELD_CUSTOM);
            pm.key(key);
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

    public static String normalizeField(@NonNull String field) {
        if (isCachePrefixed(field) || isSourcePrefixed(field)) {
            return field;
        } else if (!isEntityPrefixed(field)){
            return dot(FIELD_ENTITY) + field;
        }
        return field;
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
        return ReflectionHelper.getFieldValue(entity, field);
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
        return name.startsWith(FIELD_CACHED + ".");
    }

    public static boolean isPropertyPrefixed(@NonNull String name) {
        return name.startsWith(FIELD_PROPERTY + ".");
    }

    public static boolean isSourcePrefixed(@NonNull String name) {
        return name.startsWith(FIELD_SOURCE + ".");
    }

    public static boolean isEntityPrefixed(@NonNull String name) {
        return name.startsWith(FIELD_ENTITY + ".");
    }

    private static boolean prefixEntityField(String field) {
        return !field.startsWith(FIELD_SOURCE + ".") &&
                !field.startsWith(FIELD_ENTITY + ".") &&
                !field.startsWith(FIELD_CACHED + ".");
    }
}
