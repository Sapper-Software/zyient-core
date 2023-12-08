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

package io.zyient.core.persistence.impl.solr;

import io.zyient.base.common.model.Context;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

public class SolrContext extends Context {
    public Context splits(@NonNull String splits) {
        put(SolrDataStore.CONTEXT_KEY_JSON_SPLITS, splits);
        return this;
    }

    public String splits() {
        return (String) get(SolrDataStore.CONTEXT_KEY_JSON_SPLITS);
    }

    @SuppressWarnings("unchecked")
    public Context addMapping(@NonNull String field, @NonNull String path) {
        Map<String, String> map = (Map<String, String>) get(SolrDataStore.CONTEXT_KEY_JSON_MAP);
        if (map == null) {
            map = new HashMap<>();
            put(SolrDataStore.CONTEXT_KEY_JSON_MAP, map);
        }
        map.put(field, path);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> mappings() {
        return (Map<String, String>) get(SolrDataStore.CONTEXT_KEY_JSON_MAP);
    }
}
