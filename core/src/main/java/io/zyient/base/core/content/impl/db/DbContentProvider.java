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

package io.zyient.base.core.content.impl.db;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.content.ManagedContentProvider;
import io.zyient.base.core.content.settings.ManagedProviderSettings;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.impl.rdbms.HibernateCursor;
import io.zyient.base.core.stores.impl.rdbms.RdbmsDataStore;
import io.zyient.base.core.stores.model.Document;
import io.zyient.base.core.stores.model.DocumentId;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.hibernate.Session;

public class DbContentProvider extends ManagedContentProvider<Session> {
    protected DbContentProvider() {
        super(ManagedProviderSettings.class);
    }

    @Override
    protected void doConfigure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        super.doConfigure(xmlConfig);
        if (!(dataStore() instanceof RdbmsDataStore)) {
            throw new ConfigurationException(String.format("Invalid Data Store type. [type=%s]",
                    dataStore().getClass().getCanonicalName()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends Enum<?>, K extends IKey, D extends Document<E, K, D>> Cursor<DocumentId, Document<E, K, D>> searchDocs(AbstractDataStore.@NonNull Q query,
                                                                                                @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                int batchSize,
                                                                                                boolean download,
                                                                                                Context context) throws DataStoreException {
        RdbmsDataStore dataStore = (RdbmsDataStore) dataStore();
        HibernateCursor<DocumentId, Document<E, K, D>> cursor = (HibernateCursor<DocumentId, Document<E, K, D>>) dataStore
                .search(query,
                        batchSize,
                        DocumentId.class,
                        entityType,
                        context);
        return new DbContentCursor<>(cursor, fileSystem());
    }
}
