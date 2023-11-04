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
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.BaseSearchResult;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.stores.impl.settings.solr.SolrDbSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class SolrDataStore extends AbstractDataStore<SolrClient> {
    public static final String LITERALS_PREFIX = "literal.";
    public static final String MAP_PREFIX = "fmap.";

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
            entity.validate();
            if (entity instanceof SolrDocumentEntity) {
                ((SolrEntity<?>) entity).setCreatedTime(System.nanoTime());
                ((SolrEntity<?>) entity).setUpdatedTime(System.nanoTime());
                ContentStreamUpdateRequest ur = getContentUpdateRequest((SolrDocumentEntity) entity);
                ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
                NamedList<Object> request = client.request(ur);
                if (DefaultLogger.isTraceEnabled()) {
                    for (Map.Entry<String, Object> entry : request) {
                        DefaultLogger.trace(String.format("Response [key=%s, value=%s]",
                                entry.getKey(), entry.getValue()));
                    }
                }
                ((SolrEntity<?>) entity).getState().setState(EEntityState.Synced);
            } else if (entity instanceof SolrEntity<?>) {
                ((SolrEntity<?>) entity).setCreatedTime(System.nanoTime());
                ((SolrEntity<?>) entity).setUpdatedTime(System.nanoTime());
                UpdateResponse ur = client.addBean(entity);
                if (ur.getStatus() != 0) {
                    throw new DataStoreException(String.format("Insert failed [status=%d]. [type=%s][id=%s]",
                            ur.getStatus(), type.getCanonicalName(), entity.getKey().stringKey()));
                }
                client.commit();
                ((SolrEntity<?>) entity).getState().setState(EEntityState.Synced);
            } else {
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).setCreatedTime(System.nanoTime());
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }

                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
                }
            }

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

    private static ContentStreamUpdateRequest getContentUpdateRequest(SolrDocumentEntity entity) throws IOException {
        ContentStreamUpdateRequest ur = new ContentStreamUpdateRequest("/update/extract");
        ur.addFile(entity.getContent(), entity.getMimeType());
        ur.setParam(LITERALS_PREFIX + "id", entity.get_id());
        ur.setParam(LITERALS_PREFIX + "location", entity.getSourceLocation());
        ur.setParam(MAP_PREFIX + "content", "attr_content");
        return ur;
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
        DataStoreAuditContext ctx = new DataStoreAuditContext();
        ctx.setType(getClass().getCanonicalName());
        ctx.setName(name());
        ctx.setConnectionType(connection().getClass().getCanonicalName());
        ctx.setConnectionName(connection().name());
        return ctx;
    }

    public static String getDocumentType(@NonNull File path) throws Exception {
        TikaConfig config = TikaConfig.getDefaultConfig();
        Detector detector = config.getDetector();

        TikaInputStream stream = TikaInputStream.get(path);

        Metadata metadata = new Metadata();
        metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, path.getName());
        MediaType mediaType = detector.detect(stream, metadata);

        return mediaType.toString();
    }
}
