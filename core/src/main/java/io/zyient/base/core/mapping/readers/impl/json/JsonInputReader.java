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

package io.zyient.base.core.mapping.readers.impl.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.ReadCursor;
import io.zyient.base.core.mapping.readers.settings.JsonReaderSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonInputReader extends InputReader {
    private List<Map<String, Object>> data;
    int readIndex = 0;

    @Override
    @SuppressWarnings("unchecked")
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof JsonReaderSettings);
        try {
            ObjectMapper mapper = ((JsonReaderSettings) settings()).getObjectMapper();
            Object json = mapper.readValue(contentInfo().path(), Object.class);
            if (json != null) {
                Class<?> type = json.getClass();
                if (((JsonReaderSettings) settings()).isArray()) {
                    Object[] array = (Object[]) json;
                    data = new ArrayList<>(array.length);
                    for (Object a : array) {
                        data.add((Map<String, Object>) a);
                    }
                } else {
                    if (Strings.isNullOrEmpty(((JsonReaderSettings) settings()).getBasePath())) {
                        throw new IOException("Invalid Configuration: {basePath} not specified...");
                    }
                    if (!(json instanceof Map<?, ?>)) {
                        throw new IOException(String.format("Invalid JSON data. [type=%s]",
                                json.getClass().getCanonicalName()));
                    }
                    String path = ((JsonReaderSettings) settings()).getBasePath();
                    Object node = findNode((Map<String, Object>) json, path.split("\\."), 0);
                    if (node != null) {
                        if (node.getClass().isArray()) {
                            Object[] array = (Object[]) node;
                            data = new ArrayList<>(array.length);
                            for (Object a : array) {
                                data.add((Map<String, Object>) a);
                            }
                        }
                    }
                }
            }
            return new JsonReadCursor(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Object findNode(Map<String, Object> data, String[] parts, int index) {
        if (index == parts.length - 1) {
            return data.get(parts[index]);
        } else {
            Object node = data.get(parts[index]);
            if (node instanceof Map<?, ?>) {
                Class<?> vt = ReflectionUtils.getGenericMapValueType((Map<?, ?>) node);
                Class<?> kt = ReflectionUtils.getGenericMapKeyType((Map<?, ?>) node);
                if (vt != null && kt != null) {
                    if (vt.equals(Object.class) && kt.equals(String.class)) {
                        Map<String, Object> map = (Map<String, Object>) node;
                        return findNode(map, parts, index + 1);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<Map<String, Object>> nextBatch() throws IOException {
        if (data != null && !data.isEmpty()) {
            if (readIndex < data.size()) {
                List<Map<String, Object>> records = new ArrayList<>();
                int index = readIndex;
                for (int ii = 0; ii < settings().getReadBatchSize(); ii++) {
                    records.add(data.get(index + ii));
                    readIndex++;
                    if (readIndex >= data.size()) {
                        break;
                    }
                }
                return records;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        data = null;
    }
}
