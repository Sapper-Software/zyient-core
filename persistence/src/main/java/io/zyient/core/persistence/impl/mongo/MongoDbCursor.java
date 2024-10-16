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

package io.zyient.core.persistence.impl.mongo;

import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreException;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

public class MongoDbCursor<K extends IKey, E extends IEntity<K>> extends Cursor<K, E> {
    private final Class<? extends K> keyType;
    private final Class<? extends E> entityType;
    private final MongoDbDataStore dataStore;
    private final String query;

    public MongoDbCursor(@NonNull Class<? extends K> keyType,
                         @NonNull Class<? extends E> entityType,
                         @NonNull MongoDbDataStore dataStore,
                         @NonNull String query,
                         int currentPage) {
        super(currentPage);
        this.keyType = keyType;
        this.entityType = entityType;
        this.dataStore = dataStore;
        this.query = query;
    }

    public MongoDbCursor(@NonNull MongoDbCursor<K, E> cursor) {
        super(cursor.currentPage());
        this.keyType = cursor.keyType;
        this.entityType = cursor.entityType;
        this.dataStore = cursor.dataStore;
        this.query = cursor.query;
    }

    @Override
    protected List<E> next(int page) throws DataStoreException {
        int offset = page * pageSize();
        return dataStore.executeSearch(query, offset, pageSize(), keyType, entityType, null);
    }

    @Override
    public void close() throws IOException {

    }
}
