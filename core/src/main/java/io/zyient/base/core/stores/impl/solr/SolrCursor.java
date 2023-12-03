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

package io.zyient.base.core.stores.impl.solr;

import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.content.model.Document;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreException;
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
import java.util.Collection;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class SolrCursor<K extends IKey, E extends IEntity<K>> extends Cursor<K, E> {
    private final EntityQueryBuilder.LuceneQuery query;
    private final SolrClient client;
    private final int batchSize;
    private final Class<? extends E> entityType;

    public SolrCursor(@NonNull Class<? extends E> entityType,
                      @NonNull SolrClient client,
                      @NonNull EntityQueryBuilder.LuceneQuery query,
                      int batchSize) {
        this.entityType = entityType;
        this.client = client;
        this.query = query;
        this.batchSize = batchSize;
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
                        Object fv = doc.getFieldValue(SolrConstants.FIELD_SOLR_JSON_DATA);
                        if (fv == null) {
                            throw new DataStoreException(
                                    String.format("Search returned NULL object for key. [type=%s]",
                                            entityType.getCanonicalName()));
                        }
                        String json = null;
                        if (fv instanceof String) {
                            json = (String) fv;
                        } else if (fv instanceof Collection<?>) {
                            Collection<?> c = (Collection<?>) fv;
                            if (c.isEmpty()) {
                                throw new DataStoreException(
                                        String.format("Search returned empty array for key. [type=%s]",
                                                entityType.getCanonicalName()));
                            }
                            for (Object o : c) {
                                if (o instanceof String) {
                                    json = (String) o;
                                }
                            }
                        }
                        if (Strings.isNullOrEmpty(json)) {
                            throw new DataStoreException(
                                    String.format("Search returned empty json for key. [type=%s]",
                                            entityType.getCanonicalName()));
                        }
                        E entity = JSONUtils.read(json, entityType);
                        entities.add(entity);
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

    @Override
    public void close() throws IOException {

    }
}
