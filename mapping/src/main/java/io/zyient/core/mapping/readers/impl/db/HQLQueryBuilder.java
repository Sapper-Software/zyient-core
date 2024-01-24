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

package io.zyient.core.mapping.readers.impl.db;

import io.zyient.core.persistence.AbstractDataStore;
import lombok.NonNull;

import java.util.Map;

public class HQLQueryBuilder implements QueryBuilder{
    @Override
    public AbstractDataStore.Q build(@NonNull String query, Map<String, Object> conditions) throws Exception {
        AbstractDataStore.Q q = null;
        if (conditions != null) {
            q = new AbstractDataStore.Q()
                    .where(query)
                    .addAll(conditions);
        } else {
            q = new AbstractDataStore.Q()
                    .where(query);
        }
        return q;
    }

}
