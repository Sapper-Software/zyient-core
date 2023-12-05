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

package io.zyient.base.core.content.impl.mongo;

import dev.morphia.transactions.MorphiaSession;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.content.ManagedContentProvider;
import io.zyient.base.core.stores.model.Document;
import io.zyient.base.core.stores.model.DocumentId;
import io.zyient.base.core.content.settings.ManagedProviderSettings;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.impl.mongo.MongoDbCursor;
import io.zyient.base.core.stores.impl.mongo.MongoDbDataStore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class MongoContentProvider extends ManagedContentProvider<MorphiaSession> {
    protected MongoContentProvider() {
        super(ManagedProviderSettings.class);
    }

    @Override
    protected void doConfigure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        super.doConfigure(xmlConfig);
        if (!(dataStore() instanceof MongoDbDataStore)) {
            throw new ConfigurationException(String.format("Invalid Data Store type. [type=%s]",
                    dataStore().getClass().getCanonicalName()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends Enum<?>, K extends IKey> Cursor<DocumentId, Document<E, K>> searchDocs(AbstractDataStore.@NonNull Q query,
                                                                                                @NonNull Class<? extends Document<E, K>> entityType,
                                                                                                int batchSize,
                                                                                                boolean download,
                                                                                                Context context) throws DataStoreException {
        MongoDbDataStore dataStore = (MongoDbDataStore) dataStore();
        MongoDbCursor<DocumentId, Document<E, K>> cursor = (MongoDbCursor<DocumentId, Document<E, K>>) dataStore
                .doSearch(query,
                        batchSize,
                        DocumentId.class,
                        entityType,
                        context);
        return new MongoContentCursor<>(cursor, fileSystem());
    }
}
