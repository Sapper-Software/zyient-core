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

package io.zyient.core.mapping.readers.impl.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.impl.JacksonInputReader;
import io.zyient.core.mapping.readers.settings.JsonReaderSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonInputReader extends JacksonInputReader {

    @Override
    @SuppressWarnings("unchecked")
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof JsonReaderSettings);
        try {
            ObjectMapper mapper = ((JsonReaderSettings) settings()).getObjectMapper();
            if (((JsonReaderSettings) settings()).isArray()) {
                Object obj = mapper.readValue(contentInfo().path(), Object.class);
                if (obj != null) {
                    Object[] array = (Object[]) obj;
                    data = new ArrayList<>(array.length);
                    for (Object a : array) {
                        data.add((Map<String, Object>) a);
                    }
                }
            } else {
                Map<String, Object> obj = mapper.readValue(contentInfo().path(), Map.class);
                if (obj != null) {
                    String path = ((JsonReaderSettings) settings()).getBasePath();
                    if (!Strings.isNullOrEmpty(path)) {
                        Object node = findNode(obj, path.split("\\."), 0);
                        if (node != null) {
                            if (node.getClass().isArray()) {
                                Object[] array = (Object[]) node;
                                data = new ArrayList<>(array.length);
                                for (Object a : array) {
                                    data.add((Map<String, Object>) a);
                                }
                            } else {
                                data = List.of((Map<String, Object>) node);
                            }
                        }
                    } else {
                        data = List.of(obj);
                    }
                }
            }
            return new JsonReadCursor(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }
}
