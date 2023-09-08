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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.Charset;
import java.util.Map;

public class JSONUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

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

    public static <T> T read(byte[] data, Class<? extends T> type) throws JsonProcessingException {
        String json = new String(data, Charset.defaultCharset());
        return mapper.readValue(json, type);
    }

    public static <T> T read(String data, Class<? extends T> type) throws JsonProcessingException {
        return mapper.readValue(data, type);
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
}