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

package io.zyient.base.common.utils;


import com.expediagroup.transformer.utils.ClassUtils;
import com.expediagroup.transformer.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.reflect.ClassPath;
import io.zyient.base.common.model.PropertyModel;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility functions to help with Getting/Setting Object/Field values using Reflection.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * <p>
 * 11:10:30 AM
 */
public class ReflectionHelper {
    private static final ClassUtils CLASS_UTILS = new ClassUtils();
    private static final ReflectionUtils REFLECTION_UTILS = new ReflectionUtils();

    public static ClassUtils classUtils() {
        return CLASS_UTILS;
    }

    public static ReflectionUtils reflectionUtils() {
        return REFLECTION_UTILS;
    }

    public static Map<String, String> mapFromString(@NonNull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        String[] values = value.split(";");
        Map<String, String> map = null;
        if (values.length >= 1) {
            map = new HashMap<>();
            for (String v : values) {
                if (Strings.isNullOrEmpty(v)) continue;
                String[] kv = v.split("=");
                if (kv.length != 2) {
                    throw new Exception(String.format("Invalid map entry. [entry=%s]", v));
                }
                String key = kv[0];
                String val = kv[1];
                if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(val)) {
                    throw new Exception(String.format("Invalid map entry. [entry=%s]", v));
                }
                map.put(key.trim(), val.trim());
            }
        }
        return map;
    }

    public static Set<Class<?>> findAllClasses(String packageName) throws IOException {
        return ClassPath.from(ClassLoader.getSystemClassLoader())
                .getAllClasses()
                .stream()
                .filter(clazz -> clazz.getPackageName()
                        .equalsIgnoreCase(packageName))
                .map(ClassPath.ClassInfo::load)
                .collect(Collectors.toSet());
    }

    public static Object[] convertToObjectArray(Object array) {
        Class<?> ofArray = array.getClass().getComponentType();
        if (ofArray.isPrimitive()) {
            List<Object> ar = new ArrayList<>();
            int length = Array.getLength(array);
            for (int i = 0; i < length; i++) {
                ar.add(Array.get(array, i));
            }
            return ar.toArray();
        } else {
            return (Object[]) array;
        }
    }

    public static PropertyModel findProperty(@NonNull Class<?> type,
                                             @NonNull String property) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(property));
        if (property.indexOf('.') > 0) {
            String[] parts = property.split("\\.");
            int index = 0;
            Class<?> current = type;
            PropertyModel pm = null;
            while (index < parts.length) {
                Field field = findField(current, parts[index]);
                if (index < parts.length - 1) {
                    if (field == null) {
                        Method getter = getGetterMethod(current, parts[index]);
                        if (getter != null) {
                            current = getter.getReturnType();
                        } else {
                            break;
                        }
                    } else {
                        current = field.getType();
                    }
                } else if (index == parts.length - 1) {
                    pm = new PropertyModel()
                            .property(property)
                            .field(field)
                            .getter(getGetterMethod(current, parts[index]))
                            .setter(getSetterMethod(current, parts[index]));

                }
                index++;
            }
            return pm;
        } else {
            Field field = findField(type, property);
            return new PropertyModel()
                    .property(property)
                    .field(field)
                    .getter(getGetterMethod(type, property))
                    .setter(getSetterMethod(type, property));
        }
    }

    public static Method getSetterMethod(@NonNull Class<?> clazz, @NonNull String property) {
        List<Method> setters = CLASS_UTILS.getSetterMethods(clazz);
        if (setters != null && !setters.isEmpty()) {
            for (Method setter : setters) {
                String name = setter.getName();
                String p = StringUtils.capitalize(property);
                if (name.compareTo(String.format("set%s", p)) == 0) {
                    return setter;
                }
            }
        }
        return null;
    }

    public static Method getGetterMethod(@NonNull Class<?> clazz, @NonNull String property) {
        List<Method> getters = CLASS_UTILS.getGetterMethods(clazz);
        if (getters != null && !getters.isEmpty()) {
            for (Method getter : getters) {
                String name = getter.getName();
                String p = StringUtils.capitalize(property);
                if (name.compareTo(String.format("get%s", p)) == 0) {
                    return getter;
                }
            }
        }
        return null;
    }

    public static List<Method> findMethod(@NonNull Class<?> type,
                                          @NonNull String name,
                                          boolean includeStatic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        List<Method> methods = new ArrayList<>();
        getAllMethods(type, methods);
        if (!methods.isEmpty()) {
            List<Method> result = new ArrayList<>();
            for (Method method : methods) {
                if (method.getName().equals(name)) {
                    if (includeStatic) {
                        result.add(method);
                    } else if (!Modifier.isStatic(method.getModifiers())) {
                        result.add(method);
                    }
                }
            }
            if (!result.isEmpty()) {
                return result;
            }
        }
        return null;
    }

    private static void getAllMethods(@NonNull Class<?> type, @NonNull List<Method> methods) {
        methods.addAll(Arrays.asList(type.getDeclaredMethods()));
        Class<?> parent = type.getSuperclass();
        if (parent != null && !parent.equals(Object.class)) {
            getAllMethods(parent, methods);
        }
    }

    /**
     * Find the field with the specified name in this type or a parent type.
     *
     * @param type - Class to find the field in.
     * @param name - Field name.
     * @return - Found Field or NULL
     */
    public static Field findField(@NonNull Class<?> type,
                                  @NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

        if (name.indexOf('.') > 0) {
            String[] parts = name.split("\\.");
            int index = 0;
            Class<?> current = type;
            Field f = null;
            while (index < parts.length) {
                f = findField(current, parts[index]);
                if (f == null) break;
                current = f.getType();
                index++;
            }
            return f;
        } else {
            Field[] fields = getAllFields(type);
            if (fields != null) {
                for (Field field : fields) {
                    if (field.getName().compareTo(name) == 0) {
                        return field;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Recursively get all the declared fields for a type.
     *
     * @param type - Type to fetch fields for.
     * @return - Array of all defined fields.
     */
    public static Field[] getAllFields(@NonNull Class<?> type) {
        List<Field> fields = CLASS_UTILS.getDeclaredFields(type, true);
        if (!fields.isEmpty()) {
            Field[] fa = new Field[fields.size()];
            for (int ii = 0; ii < fields.size(); ii++) {
                fa[ii] = fields.get(ii);
            }
            return fa;
        }
        return null;
    }

    public static Map<String, Field> getFieldsMap(@NonNull Class<?> type) {
        Field[] fields = getAllFields(type);
        if (fields != null && fields.length > 0) {
            Map<String, Field> map = new HashMap<>();
            for (Field field : fields) {
                map.put(field.getName(), field);
            }
            return map;
        }
        return null;
    }

    /**
     * Check is the field value can be converted to a String value.
     *
     * @param field - Field to check type for.
     * @return - Can convert to String?
     */
    public static boolean canStringify(@NonNull Field field) {
        if (field.isEnumConstant() || field.getType().isEnum())
            return true;
        if (isPrimitiveTypeOrClass(field))
            return true;
        if (field.getType().equals(String.class))
            return true;
        if (field.getType().equals(Date.class))
            return true;
        return false;
    }

    /**
     * Check if the specified field is a primitive or primitive type class.
     *
     * @param field - Field to check primitive for.
     * @return - Is primitive?
     */
    public static boolean isPrimitiveTypeOrClass(@NonNull Field field) {
        Class<?> type = field.getType();
        return isPrimitiveTypeOrClass(type);
    }

    /**
     * Check if the specified type is a primitive or primitive type class.
     *
     * @param type - Field to check primitive for.
     * @return - Is primitive?
     */
    public static boolean isPrimitiveTypeOrClass(@NonNull Class<?> type) {
        if (isNumericType(type)) return true;
        if (type.equals(Boolean.class) || type.equals(boolean.class)) return true;
        return type.equals(Class.class);
    }

    /**
     * Check if the specified field is a primitive or primitive type class or String.
     *
     * @param field - Field to check primitive/String for.
     * @return - Is primitive or String?
     */
    public static boolean isPrimitiveTypeOrString(@NonNull Field field) {
        Class<?> type = field.getType();
        return isPrimitiveTypeOrString(type);
    }

    /**
     * Check if the specified type is a primitive or primitive type class or String.
     *
     * @param type - Field to check primitive/String for.
     * @return - Is primitive or String?
     */
    public static boolean isPrimitiveTypeOrString(@NonNull Class<?> type) {
        if (isPrimitiveTypeOrClass(type)) {
            return true;
        }
        if (type == String.class) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getValueFromString(@NonNull Class<? extends T> type,
                                           @NonNull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Object ret = null;
        if (type.equals(String.class)) {
            ret = value;
        } else if (isNumericType(type)) {
            if (isBoolean(type)) {
                ret = Boolean.parseBoolean(value);
            } else if (isShort(type)) {
                ret = Short.parseShort(value);
            } else if (isInt(type)) {
                ret = Integer.parseInt(value);
            } else if (isLong(type)) {
                ret = Long.parseLong(value);
            } else if (isFloat(type)) {
                ret = Float.parseFloat(value);
            } else if (isDouble(type)) {
                ret = Double.parseDouble(value);
            }
        } else if (type.equals(Class.class)) {
            ret = Class.forName(value);
        } else if (type.isEnum()) {
            Class<? extends Enum> etype = (Class<? extends Enum>) type;
            return (T) Enum.valueOf(etype, value);
        }
        if (ret != null) {
            return (T) ret;
        }
        throw new Exception(String.format("Type not supported. [type=%s]", type.getCanonicalName()));
    }

    public static boolean isNumericType(@NonNull Class<?> type) {
        if (isShort(type) || isInt(type) || isLong(type) || isFloat(type) || isDouble(type)) {
            return true;
        }
        return type.equals(Character.class) || type.equals(char.class);
    }

    public static boolean isDecimal(@NonNull Class<?> type) {
        return (isFloat(type) || isDouble(type));
    }

    /**
     * Check if the parent type specified is an ancestor (inheritance) of the passed type.
     *
     * @param parent - Ancestor type to check.
     * @param type   - Inherited type
     * @return - Is Ancestor type?
     */
    public static boolean isSuperType(@NonNull Class<?> parent,
                                      @NonNull Class<?> type) {
        if (parent.equals(type)) {
            return true;
        } else if (type.equals(Object.class)) {
            return false;
        } else {
            Class<?> pp = type.getSuperclass();
            if (pp == null) {
                return false;
            }
            return isSuperType(parent, pp);
        }
    }

    public static boolean isCollection(@NonNull Class<?> type) {
        return (implementsInterface(Collection.class, type));
    }

    public static boolean isCollection(@NonNull Field field) {
        return isCollection(field.getType());
    }

    public static boolean isMap(@NonNull Class<?> type) {
        return implementsInterface(Map.class, type);
    }

    public static boolean isMap(@NonNull Field field) {
        return isMap(field.getType());
    }

    /**
     * Check is the passed type (or its ancestor) implements the specified interface.
     *
     * @param intf - Interface type to check.
     * @param type - Type implementing expected interface.
     * @return - Implements Interface?
     */
    public static boolean implementsInterface(@NonNull Class<?> intf,
                                              @NonNull Class<?> type) {
        if (intf.equals(type)) {
            return true;
        }
        Class<?>[] intfs = type.getInterfaces();
        if (intfs != null && intfs.length > 0) {
            for (Class<?> itf : intfs) {
                if (isSuperType(intf, itf)) {
                    return true;
                }
            }
        }
        Class<?> parent = type.getSuperclass();
        if (parent != null && !parent.equals(Object.class)) {
            return implementsInterface(intf, parent);
        }
        return false;
    }

    /**
     * Get the Parameterized type of the Map key field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericMapKeyType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(Map.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    public static Class<?> getGenericMapKeyType(@NonNull Map<?, ?> mapObject) {
        if (!mapObject.isEmpty()) {
            for (Object key : mapObject.keySet()) {
                return key.getClass();
            }
        }
        return null;
    }

    public static Class<?> getGenericMapValueType(@NonNull Map<?, ?> mapObject) {
        if (!mapObject.isEmpty()) {
            for (Object value : mapObject.values()) {
                return value.getClass();
            }
        }
        return null;
    }

    /**
     * Get the Parameterized type of the Map value field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericMapValueType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(Map.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[1];
    }

    /**
     * Get the Parameterized type of the List field specified.
     *
     * @param field - Field to extract the Parameterized type for.
     * @return - Parameterized type.
     */
    public static Class<?> getGenericCollectionType(@NonNull Field field) {
        Preconditions
                .checkArgument(implementsInterface(Collection.class, field.getType()));

        ParameterizedType ptype = (ParameterizedType) field.getGenericType();
        return (Class<?>) ptype.getActualTypeArguments()[0];
    }

    /**
     * Get the parsed value of the type specified from the
     * string value passed.
     *
     * @param type  - Required value type
     * @param value - Input String value
     * @return - Parsed Value.
     */
    @SuppressWarnings("unchecked")
    public static Object parseStringValue(Class<?> type, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            if (isPrimitiveTypeOrString(type)) {
                return parsePrimitiveValue(type, value);
            } else if (type.isEnum()) {
                Class<Enum> et = (Class<Enum>) type;
                return Enum.valueOf(et, value);
            }
        }
        return null;
    }

    /**
     * Get the value of the primitive type parsed from the string value.
     *
     * @param type  - Primitive Type
     * @param value - String value
     * @return - Parsed Value
     */
    private static Object parsePrimitiveValue(Class<?> type, String value) {
        if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            return Boolean.parseBoolean(value);
        } else if (type.equals(Short.class) || type.equals(short.class)) {
            return Short.parseShort(value);
        } else if (type.equals(Integer.class) || type.equals(int.class)) {
            return Integer.parseInt(value);
        } else if (type.equals(Long.class) || type.equals(long.class)) {
            return Long.parseLong(value);
        } else if (type.equals(Float.class) || type.equals(float.class)) {
            return Float.parseFloat(value);
        } else if (type.equals(Double.class) || type.equals(double.class)) {
            return Double.parseDouble(value);
        } else if (type.equals(Character.class) || type.equals(char.class)) {
            return value.charAt(0);
        } else if (type.equals(Byte.class) || type.equals(byte.class)) {
            return Byte.parseByte(value);
        } else if (type.equals(String.class)) {
            return value;
        }
        return null;
    }

    public static Constructor<?> getConstructor(Class<?> type, Class<?>... args) throws ReflectionException, NoSuchMethodException {
        Constructor<?>[] constructors = type.getConstructors();
        int le = (args == null ? 0 : args.length);
        if (le == 0) {
            for (Constructor<?> ctor : constructors) {
                if (ctor.getGenericParameterTypes().length == le) {
                    return ctor;
                }
            }
        } else {
            return type.getDeclaredConstructor(args);
        }
        throw new ReflectionException(String.format("No matching constructor found. [type=%s][args=%s]",
                type.getCanonicalName(), (args == null ? "NONE" : Arrays.toString(args))));
    }

    public static boolean isBoolean(@NonNull Class<?> type) {
        return (type.equals(Boolean.TYPE) || type.equals(Boolean.class) || type.equals(boolean.class));
    }

    public static boolean isShort(@NonNull Class<?> type) {
        return (type.equals(Short.TYPE) || type.equals(Short.class) || type.equals(short.class));
    }

    public static boolean isInt(@NonNull Class<?> type) {
        return (type.equals(Integer.TYPE) || type.equals(Integer.class) || type.equals(int.class));
    }

    public static boolean isLong(@NonNull Class<?> type) {
        return (type.equals(Long.TYPE) || type.equals(Long.class) || type.equals(long.class));
    }

    public static boolean isFloat(@NonNull Class<?> type) {
        return (type.equals(Float.TYPE) || type.equals(Float.class) || type.equals(float.class));
    }

    public static boolean isDouble(@NonNull Class<?> type) {
        return (type.equals(Double.TYPE) || type.equals(Double.class) || type.equals(double.class));
    }

    public static boolean isByte(@NonNull Class<?> type) {
        return (type.equals(byte.class) || type.equals(Byte.class) || type.equals(Byte.TYPE));
    }

    public static boolean isChar(@NonNull Class<?> type) {
        return (type.equals(char.class) || type.equals(Character.class) || type.equals(Character.TYPE));
    }

    public static <T> T createInstance(Class<T> type) throws Exception {
        Constructor<T> constructor = type.getDeclaredConstructor();
        return constructor.newInstance();
    }
}
