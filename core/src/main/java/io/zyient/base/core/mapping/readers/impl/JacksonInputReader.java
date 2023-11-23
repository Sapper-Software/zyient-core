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

package io.zyient.base.core.mapping.readers.impl;

import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.readers.InputReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class JacksonInputReader extends InputReader {
    protected List<Map<String, Object>> data;
    protected int readIndex = 0;

    @SuppressWarnings("unchecked")
    protected Object findNode(Map<String, Object> data, String[] parts, int index) {
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
