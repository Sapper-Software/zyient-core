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
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.model.StringKey;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.BaseSearchResult;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.VersionedEntity;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.stores.impl.settings.solr.SolrDbSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import software.amazon.awssdk.utils.StringInputStream;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;


public class SolrDataStore extends AbstractDataStore<SolrClient> {
    public static final String LITERALS_PREFIX = "literal.";
    public static final String MAP_PREFIX = "fmap.";
    public static final String CONTEXT_KEY_JSON_MAP = "SOLR_JSON_FIELDS";
    public static final String CONTEXT_KEY_JSON_SPLITS = "SOLR_JSON_SPLITS";

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
                if (((SolrDocumentEntity) entity).getCreatedTime() <= 0)
                    ((SolrEntity<?>) entity).setCreatedTime(System.nanoTime());
                ((SolrEntity<?>) entity).setUpdatedTime(System.nanoTime());
                if (Strings.isNullOrEmpty(((SolrDocumentEntity) entity).getMimeType())) {
                    String mimeType = getDocumentType(((SolrDocumentEntity) entity).getContent());
                    ((SolrDocumentEntity) entity).setMimeType(mimeType);
                }
                ContentStreamUpdateRequest ur = getContentUpdateRequest((SolrDocumentEntity) entity);
                NamedList<Object> request = client.request(ur);
                if (DefaultLogger.isTraceEnabled()) {
                    for (Map.Entry<String, Object> entry : request) {
                        DefaultLogger.trace(String.format("Response [key=%s, value=%s]",
                                entry.getKey(), entry.getValue()));
                    }
                }
                ((SolrEntity<?>) entity).getState().setState(EEntityState.Synced);
            } else if (entity instanceof SolrEntity<?>) {
                if (((SolrEntity<?>) entity).getCreatedTime() <= 0)
                    ((SolrEntity<?>) entity).setCreatedTime(System.nanoTime());
                ((SolrEntity<?>) entity).setUpdatedTime(System.nanoTime());
                UpdateResponse ur = client.addBean(entity);
                if (ur.getStatus() != 0) {
                    throw new DataStoreException(String.format("Insert failed [status=%d]. [type=%s][id=%s]",
                            ur.getStatus(), type.getCanonicalName(), entity.entityKey().stringKey()));
                }
                ((SolrEntity<?>) entity).getState().setState(EEntityState.Synced);
            } else {
                if (entity instanceof BaseEntity<?>) {
                    if (((BaseEntity<?>) entity).getCreatedTime() <= 0)
                        ((BaseEntity<?>) entity).setCreatedTime(System.nanoTime());
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }
                JSONUpdateRequest ur = getJsonUpdateRequest(entity, context);
                NamedList<Object> request = client.request(ur);
                if (DefaultLogger.isTraceEnabled()) {
                    for (Map.Entry<String, Object> entry : request) {
                        DefaultLogger.trace(String.format("Response [key=%s, value=%s]",
                                entry.getKey(), entry.getValue()));
                    }
                }
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
                }
            }
            client.commit();
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
        checkState();
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(type);
            SolrClient client = connection.connect(cname);
            entity.validate();
            if (checkEntityVersion(context)) {
                checkEntity(entity, client);
            }
            return createEntity(entity, type, context);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <E extends IEntity<?>> void checkEntity(E entity, SolrClient client) throws Exception {
        E current = (E) findEntity(entity.entityKey(), entity.getClass(), null);
        if (current == null) {
            throw new DataStoreException(String.format("Entity not found. [type=%s][key=%s]",
                    entity.getClass().getCanonicalName(), entity.entityKey().stringKey()));
        }
        if (entity instanceof VersionedEntity) {
            if (((VersionedEntity) entity).version() != ((VersionedEntity) current).version()) {
                throw new DataStoreException(String.format("Entity version is stale. [current=%d][expected=%d]",
                        ((VersionedEntity) current).version(), ((VersionedEntity) entity).version()));
            }
        }
        UpdateResponse ur = client.deleteById(entity.entityKey().stringKey());
        if (ur.getStatus() != 0) {
            throw new DataStoreException(String.format("Delete failed [status=%d]. [type=%s][id=%s]",
                    ur.getStatus(), entity.getClass().getCanonicalName(), entity.entityKey().stringKey()));
        }
    }

    @Override
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws DataStoreException {
        checkState();
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(type);
            SolrClient client = connection.connect(cname);
            String k = null;
            if (key instanceof String) {
                k = (String) key;
            } else if (key instanceof IKey) {
                k = ((IKey) key).stringKey();
            } else {
                throw new DataStoreException(String.format("Key type not supported. [type=%s]",
                        key.getClass().getCanonicalName()));
            }
            if (checkEntityVersion(context)) {
                E current = (E) findEntity(key, type, null);
                if (current == null) {
                    throw new DataStoreException(String.format("Entity not found. [type=%s][key=%s]",
                            type.getCanonicalName(), k));
                }
            }
            UpdateResponse ur = client.deleteById(k);
            if (ur.getStatus() != 0) {
                throw new DataStoreException(String.format("Delete failed [status=%d]. [type=%s][id=%s]",
                        ur.getStatus(), type.getCanonicalName(), k));
            }
            client.commit();
            return true;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends IEntity<?>> JSONUpdateRequest getJsonUpdateRequest(E entity,
                                                                                 Context context) throws Exception {
        String json = JSONUtils.asString(entity, entity.getClass());
        JSONUpdateRequest ur = new JSONUpdateRequest(new StringInputStream(json), "/update/json/docs");
        if (context != null) {
            if (context.containsKey(CONTEXT_KEY_JSON_MAP)) {
                Object o = context.get(CONTEXT_KEY_JSON_MAP);
                if (o instanceof Map<?, ?>) {
                    Map<String, String> map = (Map<String, String>) o;
                    for (String key : map.keySet()) {
                        String value = map.get(key);
                        ur.addFieldMapping(key, value);
                    }
                } else {
                    throw new Exception(String.format("Invalid context object. [key=%s]", CONTEXT_KEY_JSON_MAP));
                }
            }
            if (context.containsKey(CONTEXT_KEY_JSON_SPLITS)) {
                String s = (String) context.get(CONTEXT_KEY_JSON_SPLITS);
                ur.setSplit(s);
            }
        }
        ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        return ur;
    }

    private static ContentStreamUpdateRequest getContentUpdateRequest(SolrDocumentEntity entity) throws IOException {
        ContentStreamUpdateRequest ur = new ContentStreamUpdateRequest("/update/extract");
        ur.addFile(entity.getContent(), entity.getMimeType());
        ur.setParam(LITERALS_PREFIX + "id", entity.get_id());
        ur.setParam(LITERALS_PREFIX + "location", entity.getSourceLocation());
        ur.setParam(MAP_PREFIX + "content", "attr_content");
        ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        return ur;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               Context context) throws DataStoreException {
        checkState();
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(type);
            SolrClient client = connection.connect(cname);
            String k = null;
            if (key instanceof String) {
                k = (String) key;
            } else if (key instanceof IKey) {
                k = ((IKey) key).stringKey();
            } else {
                throw new DataStoreException(String.format("Key type not supported. [type=%s]",
                        key.getClass().getCanonicalName()));
            }
            if (ReflectionUtils.isSuperType(SolrEntity.class, type)) {
                SolrQuery q = new SolrQuery();
                q.set("q", getQueryString(SolrEntity.FIELD_SOLR_ID, k, String.class));
                final QueryResponse response = client.query(q);
                List<E> entities = (List<E>) response.getBeans(type);
                if (entities != null) {
                    if (entities.size() > 1) {
                        throw new DataStoreException(
                                String.format("Multiple entries found for key. [type=%s][key=%s]",
                                        type.getCanonicalName(), k));
                    }
                    SolrEntity<?> entity = (SolrEntity<?>) entities.get(0);
                    entity.getState().setState(EEntityState.Synced);
                    return (E) entity;
                }
            } else {
                SolrQuery q = new SolrQuery();
                q.set("q", getQueryString(SolrEntity.FIELD_SOLR_ID, k, String.class));
                final QueryResponse response = client.query(q);
                SolrDocumentList docs = response.getResults();
                if (docs != null) {
                    if (docs.size() > 1) {
                        throw new DataStoreException(
                                String.format("Multiple entries found for key. [type=%s][key=%s]",
                                        type.getCanonicalName(), k));
                    }
                    SolrDocument doc = docs.get(0);
                }
            }
            return null;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    public static String getQueryString(@NonNull String field,
                                        @NonNull Object value,
                                        @NonNull Class<?> type) {
        if (ReflectionUtils.isDecimal(type)) {
            return String.format("%s:%f", field, (double) value);
        } else if (ReflectionUtils.isNumericType(type)) {
            return String.format("%s:%d", field, (long) value);
        }
        return String.format("%s:\"%s\"", field, value);
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

    public static Context createContext() throws Exception {
        return defaultContext(SolrContext.class);
    }
}
