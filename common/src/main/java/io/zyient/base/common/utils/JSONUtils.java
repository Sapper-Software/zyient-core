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
import java.nio.charset.Charset;
import java.util.Map;

public class JSONUtils {

    private static final ObjectMapper mapper = GlobalConstants.getJsonMapper();

    public static ObjectMapper mapper() {
        return mapper;
    }

    public static byte[] asBytes(@NonNull Object obj, @NonNull Class<?> type) throws JsonProcessingException {
        String json = mapper.writeValueAsString(obj);
        if (!Strings.isNullOrEmpty(json)) {
            return json.getBytes(Charset.defaultCharset());
        }
        return null;
    }

    public static String asString(@NonNull Object obj, @NonNull Class<?> type) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
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
        byte[] data = asBytes(value, value.getClass());
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
}
