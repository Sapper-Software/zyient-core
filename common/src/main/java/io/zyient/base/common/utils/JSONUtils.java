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

package io.zyient.base.common.utils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.zjsonpatch.JsonDiff;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class JSONUtils {

    private static final ObjectMapper mapper = GlobalConstants.getJsonMapper();

    public static ObjectMapper mapper() {
        return mapper;
    }

    public static byte[] asBytes(@NonNull Object obj) throws JsonProcessingException {
        String json = mapper.writeValueAsString(obj);
        if (!Strings.isNullOrEmpty(json)) {
            return json.getBytes(Charset.defaultCharset());
        }
        return null;
    }

    public static String asString(@NonNull Object obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(@NonNull Object obj) throws JsonProcessingException {
        String json = asString(obj);
        return read(json, Map.class);
    }

    public static <T> T read(byte[] data, Class<? extends T> type) throws Exception {
        String json = new String(data, Charset.defaultCharset());
        if (Strings.isNullOrEmpty(json)) return null;
        if (NetUtils.isIPV4Address(json)) return null;
        if (ReflectionHelper.isPrimitiveTypeOrString(type)) {
            return ReflectionHelper.getValueFromString(type, json);
        }
        if (!isJson(json)) {
            return null;
        }
        return mapper.readValue(json, type);
    }

    public static <T> T read(String data, Class<? extends T> type) throws JsonProcessingException {
        return mapper.readValue(data, type);
    }

    public static <T> T read(@NonNull File source, Class<? extends T> type) throws IOException {
        return mapper.readValue(source, type);
    }

    public static boolean isJson(@NonNull String value) {
        if (!Strings.isNullOrEmpty(value)) {
            try {
                mapper.readValue(value, Map.class);
                return true;
            } catch (Exception ex) {
                return false;
            }
        }
        return false;
    }

    public static <T> T read(@NonNull CuratorFramework client,
                             @NonNull String path,
                             @NonNull Class<? extends T> type) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                return JSONUtils.read(data, type);
            }
        }
        return null;
    }

    public static void write(@NonNull CuratorFramework client,
                             @NonNull String path,
                             @NonNull Object value) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path);
        }
        byte[] data = asBytes(value);
        client.setData().forPath(path, data);
    }

    @SuppressWarnings("unchecked")
    public static Object find(@NonNull Map<String, Object> data,
                              @NonNull String path) throws Exception {
        if (path.indexOf('.') < 0) {
            return data.get(path);
        } else {
            String[] parts = path.split("\\.");
            Object current = data;
            int index = 0;
            while (index < parts.length) {
                if (current == null) break;
                String key = parts[index];
                if (current instanceof Map<?, ?>) {
                    Map<String, ?> map = (Map<String, ?>) current;
                    current = map.get(key);
                } else {
                    throw new Exception(String.format("Invalid path [%s][key=%s][type=%s]",
                            path, key, current.getClass().getCanonicalName()));
                }
                index++;
            }
            return current;
        }
    }

    public static <T> String diff(@NonNull T current,
                                  @NonNull T previous) throws Exception {
        ObjectMapper mapper = mapper();
        JsonNode cn = mapper.valueToTree(current);
        JsonNode pn = mapper.valueToTree(previous);
        JsonNode diff = JsonDiff.asJson(cn, pn);
        return mapper.writeValueAsString(diff);
    }

    public static void checkAndAddType(@NonNull Map<String, Object> map,
                                       @NonNull Class<?> type) {
        if (type.isAnnotationPresent(JsonTypeInfo.class)) {
            JsonTypeInfo ti = type.getAnnotation(JsonTypeInfo.class);
            String key = ti.property();
            if (Strings.isNullOrEmpty(key)) {
                key = ti.use().getDefaultPropertyName();
            }
            map.put(key, type.getTypeName());
        }
    }

    public static Object createBlankJson(@NonNull Class<?> type) throws Exception {
        Object o = ReflectionHelper.createInstance(type);
        Field[] fields = ReflectionHelper.getAllFields(type);
        for (Field field : fields) {
            if (ReflectionHelper.isPrimitiveTypeOrString(field)) {
                if (ReflectionHelper.isBoolean(field.getType())) {
                    ReflectionHelper.setBooleanValue(o, field, false);
                } else if (ReflectionHelper.isDouble(field.getType())) {
                    ReflectionHelper.setDoubleValue(o, field, 0.0d);
                } else if (ReflectionHelper.isLong(field.getType())) {
                    ReflectionHelper.setLongValue(o, field, 0L);
                } else if (ReflectionHelper.isInt(field.getType())) {
                    ReflectionHelper.setIntValue(o, field, 0);
                } else {
                    ReflectionHelper.setStringValue(o, field, "");
                }

            } else if (ReflectionHelper.isCollection(field)) {
                Object noList = ReflectionHelper.createInstance(ArrayList.class);
                ReflectionHelper.setObjectValue(o, field, noList);
                Object no = createBlankJson(ReflectionHelper.getGenericCollectionType(field));
                Collection<Object> c = (Collection<Object>) noList;
                c.add(no);
                ReflectionHelper.setObjectValue(o, field, c);
            } else {
                Object no = createBlankJson(field.getType());
                ReflectionHelper.setObjectValue(o, field, no);
            }
        }
        return o;
    }
}
