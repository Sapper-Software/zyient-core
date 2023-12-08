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

import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Getter
@Accessors(fluent = true)
public class SolrCursor<K extends IKey, E extends IEntity<K>> extends Cursor<K, E> {
    private final EntityQueryBuilder.LuceneQuery query;
    private final int batchSize;
    private final Class<? extends E> entityType;
    private final SolrDataStore dataStore;
    private final SolrClient client;
    private final boolean fetchChildren;

    public SolrCursor(@NonNull Class<? extends E> entityType,
                      @NonNull SolrDataStore dataStore,
                      @NonNull SolrClient client,
                      @NonNull EntityQueryBuilder.LuceneQuery query,
                      int batchSize,
                      boolean fetchChildren) {
        this.entityType = entityType;
        this.dataStore = dataStore;
        this.client = client;
        this.query = query;
        this.batchSize = batchSize;
        this.fetchChildren = fetchChildren;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<E> next(int page) throws DataStoreException {
        try {
            SolrQuery query = new SolrQuery(this.query.where());
            query.setStart(page * batchSize);
            query.setRows(batchSize);
            QueryResponse response = client.query(query);
            if (ReflectionHelper.isSuperType(SolrEntity.class, entityType)) {
                List<E> entities = (List<E>) response.getBeans(entityType);
                if (entities != null) {
                    if (!entities.isEmpty()) {
                        for (E entity : entities) {
                            ((SolrEntity<?>) entity).getState().setState(EEntityState.Synced);
                        }
                    }
                }
                return entities;
            } else if (ReflectionHelper.isSuperType(Document.class, entityType)) {
                SolrDocumentList documents = response.getResults();
                if (documents != null && !documents.isEmpty()) {
                    List<E> entities = new ArrayList<>(documents.size());
                    for (SolrDocument doc : documents) {
                        Document<?, ?, ?> document = dataStore().readDocument(doc,
                                this.query.where(),
                                (Class<? extends Document<?, ?, ?>>) entityType,
                                fetchChildren);
                        entities.add((E) document);
                    }
                    return entities;
                }
            } else {
                List<SolrJsonEntity> entities = response.getBeans(SolrJsonEntity.class);
                if (entities != null && !entities.isEmpty()) {
                    List<E> array = new ArrayList<>(entities.size());
                    for (SolrJsonEntity je : entities) {
                        E entity = JSONUtils.read(je.getJson(), entityType);
                        array.add(entity);
                    }
                    return array;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public Set<Document<?, ?, ?>> fetchChildren(@NonNull DocumentId id,
                                             @NonNull Class<? extends Document<?, ?, ?>> type) throws DataStoreException {
        try {
            return dataStore.fetchChildren(id, type, fetchChildren);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
