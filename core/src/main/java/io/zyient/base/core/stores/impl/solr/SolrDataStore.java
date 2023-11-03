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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.BaseSearchResult;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.stores.impl.settings.solr.SolrDbSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.solr.client.solrj.SolrClient;

import java.util.Map;

public class SolrDataStore extends AbstractDataStore<SolrClient> {
    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkArgument(settings instanceof SolrDbSettings);
        try {
            SolrConnection connection = (SolrConnection) connection();
            if (!connection.isConnected()) {
                connection.connect();
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private String getCollection(Class<?> type) throws Exception {
        if (type.isAnnotationPresent(SolrCollection.class)) {
            SolrCollection sc = type.getAnnotation(SolrCollection.class);
            String name = sc.value();
            if (Strings.isNullOrEmpty(name)) {
                name = type.getSimpleName();
            }
            return name.toLowerCase();
        }
        throw new Exception(String.format("Annotation not found. [type=%s]", type.getCanonicalName()));
    }

    @Override
    public <E extends IEntity<?>> E createEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        checkState();
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(type);
            SolrClient client = connection.connect(cname);


            return entity;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws DataStoreException {
        return false;
    }

    @Override
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               @NonNull Class<? extends E> type,
                                                               Context context) throws DataStoreException {
        return null;
    }

    @Override
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               Map<String, Object> parameters,
                                                               @NonNull Class<? extends E> type,
                                                               Context context) throws DataStoreException {
        return null;
    }

    @Override
    public DataStoreAuditContext context() {
        return null;
    }
}
