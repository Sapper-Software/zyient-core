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

package io.zyient.core.content.impl.indexed;

import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.impl.solr.DocumentQueryBuilder;
import io.zyient.core.persistence.impl.solr.SolrCursor;
import io.zyient.core.persistence.impl.solr.SolrDataStore;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.core.persistence.model.DocumentState;
import io.zyient.core.content.DocumentContext;
import io.zyient.core.content.ManagedContentProvider;
import io.zyient.core.content.settings.ManagedProviderSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.solr.client.solrj.SolrClient;

import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class IndexedContentProvider extends ManagedContentProvider<SolrClient> {

    protected IndexedContentProvider() {
        super(ManagedProviderSettings.class);
    }

    @Override
    protected void doConfigure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        super.doConfigure(xmlConfig);
        if (!(dataStore() instanceof SolrDataStore)) {
            throw new ConfigurationException(String.format("Invalid Data Store type. [type=%s]",
                    dataStore().getClass().getCanonicalName()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Document<E, K, D> findDoc(@NonNull Map<String, String> uri,
                                                                                                                  @NonNull String collection,
                                                                                                                  @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                                  DocumentContext context) throws DataStoreException {
        try {
            AbstractDataStore.Q query = DocumentQueryBuilder.build(entityType, collection, uri);
            SolrDataStore dataStore = (SolrDataStore) dataStore();
            try (SolrCursor<DocumentId, Document<E, K, D>> cursor = (SolrCursor<DocumentId, Document<E, K, D>>) dataStore
                    .search(query,
                            8,
                            DocumentId.class,
                            entityType,
                            context)) {
                List<Document<E, K, D>> documents = cursor.nextPage();
                if (documents != null && !documents.isEmpty()) {
                    if (documents.size() > 1) {
                        throw new DataStoreException(String.format("Multiple documents found for path. [uri=%s]", uri));
                    }
                    return documents.get(0);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Cursor<DocumentId, Document<E, K, D>> searchDocs(AbstractDataStore.@NonNull Q query,
                                                                                                                                         @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                                                         int batchSize,
                                                                                                                                         boolean download,
                                                                                                                                         DocumentContext context) throws DataStoreException {
        SolrDataStore dataStore = (SolrDataStore) dataStore();
        SolrCursor<DocumentId, Document<E, K, D>> cursor = (SolrCursor<DocumentId, Document<E, K, D>>) dataStore
                .search(query,
                        batchSize,
                        DocumentId.class,
                        entityType,
                        context);
        return new SolrContentCursor<>(cursor, fileSystem());
    }
}
