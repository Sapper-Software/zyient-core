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

import com.google.common.base.Preconditions;
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
    public static final String FIELD_CUSTOM = "entity.property";
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
            if (var.startsWith("custom.")) {
                var = var.replace("custom\\.", FIELD_CUSTOM);
            } else if (prefixEntityField(var)) {
                var = FIELD_ENTITY + "." + var;
            }
            fields.put(exp, var);
        }
        return fields;
    }

    public static PropertyModel findField(@NonNull String name,
                                          @NonNull Class<?> entityType) throws Exception {
        if (name.startsWith(prefixed(FIELD_CACHED))) {
            return ReflectionHelper.findProperty(MappedResponse.class, name);
        } else if (name.startsWith(prefixed(FIELD_SOURCE))) {
            return ReflectionHelper.findProperty(MappedResponse.class, name);
        } else if (name.startsWith(prefixed(FIELD_CUSTOM))) {
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
            return ReflectionHelper.findProperty(entityType, name);
        }
    }

    public static void setProperty(@NonNull PropertyModel property,
                                   @NonNull Object entity,
                                   Object value) throws Exception {
        Preconditions.checkNotNull(property.setter());
        if (property instanceof ExtendedPropertyModel) {
            property.setter().invoke(entity, ((ExtendedPropertyModel) property).key(), value);
        } else {
            property.setter().invoke(entity, value);
        }
    }

    public static Object getProperty(@NonNull PropertyModel property,
                                     @NonNull Object entity) throws Exception {
        Preconditions.checkNotNull(property.getter());
        if (property instanceof ExtendedPropertyModel) {
            return property.getter().invoke(entity, ((ExtendedPropertyModel) property).key());
        } else {
            return property.getter().invoke(entity);
        }
    }

    private static String prefixed(String name) {
        if (!name.endsWith(".")) {
            name = name + ".";
        }
        return name;
    }

    private static String removePrefix(String name, String prefix) {
        prefix = prefixed(prefix);
        if (name.startsWith(prefix))
            return name.replaceFirst(prefix, "");
        return name;
    }

    private static boolean prefixEntityField(String field) {
        return !field.startsWith(FIELD_SOURCE) &&
                !field.startsWith(FIELD_ENTITY) &&
                !field.startsWith(FIELD_CACHED);
    }
}
