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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.persistence.model.BaseEntity;
import io.zyient.core.persistence.*;
import io.zyient.core.persistence.impl.settings.solr.SolrDbSettings;
import io.zyient.core.persistence.impl.solr.schema.SolrFieldTypes;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.base.core.utils.FileUtils;
import io.zyient.core.persistence.model.VersionedEntity;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;


public class SolrDataStore extends AbstractDataStore<SolrClient> {
    public static class SchemaConstants {
        public static final String NAME = "name";
        public static final String TYPE = "type";
        public static final String STORED = "stored";
        public static final String INDEXED = "indexed";
        public static final String DOC_VALUES = "docValues";
    }

    public static class SchemaScanResponse {
        private final Map<String, Map<String, Object>> added = new HashMap<>();
        private final Map<String, Map<String, Object>> updated = new HashMap<>();
        private final Set<Class<?>> scanned = new HashSet<>();
    }

    public static final String SOLR_URL_UPDATE = "/update";
    public static final String SOLR_URL_EXTRACT = String.format("%s/extract", SOLR_URL_UPDATE);
    public static final String LITERALS_PREFIX = "literal.";
    public static final String MAP_PREFIX = "fmap.";
    public static final String CONTEXT_KEY_JSON_MAP = "SOLR_JSON_FIELDS";
    public static final String CONTEXT_KEY_JSON_SPLITS = "SOLR_JSON_SPLITS";
    public static final String JSON_FIELD = "data_json";
    private final Map<Class<?>, Boolean> checkedSchema = new HashMap<>();

    @Override
    protected <K extends IKey, E extends IEntity<K>> QueryParser<K, E> createParser(@NonNull Class<? extends E> entityType,
                                                                                    @NonNull Class<? extends K> keyTpe) throws Exception {
        throw new Exception("Query Parser not available...");
    }

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

    private <E extends IEntity<?>> String getCollection(E entity) throws Exception {
        if (entity instanceof Document<?, ?, ?>) {
            return ((Document<?, ?, ?>) entity).getId().getCollection();
        }
        return getCollection(entity.getClass());
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

    private String getCollection(Object key, Class<?> type) throws Exception {
        if (key instanceof DocumentId) {
            return ((DocumentId) key).getCollection();
        }
        return getCollection(type);
    }

    @Override
    public <E extends IEntity<?>> E createEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        checkState();
        return createEntity(entity, null, type, context);
    }

    private <E extends IEntity<?>> E createEntity(E entity,
                                                  E current,
                                                  Class<? extends E> type,
                                                  Context context) throws DataStoreException {
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(entity);
            SolrClient client = connection.connect(cname);
            entity.validate();
            if (!checkedSchema.containsKey(type)) {
                checkSchema(type, client);
            }
            if (entity instanceof Document<?, ?, ?>) {
                if (((Document<?, ?, ?>) entity).getCreatedTime() <= 0)
                    ((Document<?, ?, ?>) entity).setCreatedTime(System.nanoTime());
                ((Document<?, ?, ?>) entity).setUpdatedTime(System.nanoTime());
                if (Strings.isNullOrEmpty(((Document<?, ?, ?>) entity).getMimeType())) {
                    String mimeType = getDocumentType(((Document<?, ?, ?>) entity).getPath());
                    ((Document<?, ?, ?>) entity).setMimeType(mimeType);
                }
                ((Document<?, ?, ?>) entity).setSearchId(((Document<?, ?, ?>) entity).getId().stringKey());
                if (((Document<?, ?, ?>) entity).getReferenceId() != null) {
                    ((Document<?, ?, ?>) entity)
                            .setSearchReferenceId(((Document<?, ?, ?>) entity).getReferenceId().stringKey());
                }
                ((Document<?, ?, ?>) entity).getState().setState(EEntityState.Synced);
                ContentStreamUpdateRequest ur = getContentUpdateRequest((Document<?, ?, ?>) entity);
                NamedList<Object> request = client.request(ur);
                if (DefaultLogger.isTraceEnabled()) {
                    for (Map.Entry<String, Object> entry : request) {
                        DefaultLogger.trace(String.format("Response [key=%s, value=%s]",
                                entry.getKey(), entry.getValue()));
                    }
                }
            } else if (entity instanceof SolrEntity<?>) {
                ((SolrEntity<?>) entity).setId(entity.entityKey().stringKey());
                if (((SolrEntity<?>) entity).getCreatedTime() <= 0)
                    ((SolrEntity<?>) entity).setCreatedTime(System.nanoTime());
                ((SolrEntity<?>) entity).setUpdatedTime(System.nanoTime());
                ((SolrEntity<?>) entity).getState().setState(EEntityState.Synced);
                UpdateResponse ur = client.addBean(entity);
                if (ur.getStatus() != 0) {
                    throw new DataStoreException(String.format("Insert failed [status=%d]. [type=%s][id=%s]",
                            ur.getStatus(), type.getCanonicalName(), entity.entityKey().stringKey()));
                }
            } else {
                long createdTimestamp = System.nanoTime();
                long updatedTimestamp = System.nanoTime();
                if (entity instanceof BaseEntity<?>) {
                    if (((BaseEntity<?>) entity).getCreatedTime() <= 0) {
                        ((BaseEntity<?>) entity).setCreatedTime(createdTimestamp);
                    } else {
                        createdTimestamp = ((BaseEntity<?>) entity).getCreatedTime();
                    }
                    ((BaseEntity<?>) entity).setUpdatedTime(updatedTimestamp);
                    ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
                }
                SolrJsonEntity je = new SolrJsonEntity();
                je.setId(entity.entityKey().stringKey());
                je.setJson(JSONUtils.asString(entity, entity.getClass()));
                je.setCreatedTime(createdTimestamp);
                je.setUpdatedTime(updatedTimestamp);

                UpdateResponse ur = client.addBean(je);
                if (ur.getStatus() != 0) {
                    throw new DataStoreException(String.format("Insert failed [status=%d]. [type=%s][id=%s]",
                            ur.getStatus(), type.getCanonicalName(), entity.entityKey().stringKey()));
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
            String cname = getCollection(entity);
            SolrClient client = connection.connect(cname);
            entity.validate();
            E current = null;
            if (checkEntityVersion(context)) {
                current = checkEntity(entity, client);
            }
            if (entity instanceof Document<?, ?, ?>) {
                Set<? extends Document<?, ?, ?>> children = ((Document<?, ?, ?>) entity).getDocuments();
                if (children != null && !children.isEmpty()) {
                    if (current == null) {
                        current = findEntity(entity.entityKey(), type, context);
                        if (current == null) {
                            throw new DataStoreException(String.format("Existing instance not found. [id=%s]",
                                    entity.entityKey().stringKey()));
                        }
                    }
                }
            }
            return createEntity(entity, current, type, context);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <E extends IEntity<?>> E checkEntity(E entity, SolrClient client) throws Exception {
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
        return current;
    }

    @Override
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws DataStoreException {
        checkState();
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(key, type);
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
            E current = (E) findEntity(key, type, null);
            if (current == null) {
                throw new DataStoreException(String.format("Entity not found. [type=%s][key=%s]",
                        type.getCanonicalName(), k));
            }
            if (current instanceof Document<?, ?, ?>) {
                if (((Document<?, ?, ?>) current).getDocumentCount() > 0) {
                    Set<? extends Document<?, ?, ?>> children = ((Document<?, ?, ?>) current).getDocuments();
                    if (children != null && !children.isEmpty()) {
                        for (Document<?, ?, ?> child : children) {
                            if (!deleteEntity(child.entityKey(), type, context)) {
                                DefaultLogger.warn(String.format("Failed to delete nested document. [id=%s]",
                                        child.entityKey().stringKey()));
                            }
                        }
                    }
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

    private void checkSchema(Class<?> type,
                             SolrClient client) throws Exception {
        SchemaRequest request = new SchemaRequest();
        SchemaResponse response = request.process(client);
        SchemaRepresentation schema = response.getSchemaRepresentation();
        List<Map<String, Object>> fields = schema.getFields();
        Map<String, Map<String, Object>> fieldMap = new HashMap<>();
        for (Map<String, Object> field : fields) {
            fieldMap.put((String) field.get(SchemaConstants.NAME), field);
        }
        SchemaScanResponse result = new SchemaScanResponse();
        scanType(type, result, fieldMap);
        if (!result.added.isEmpty()) {
            addSchemaFields(result.added, client);
        }
        if (!result.updated.isEmpty()) {
            updateSchemaFields(result.updated, client);
        }
        checkedSchema.put(type, true);
    }

    private void addSchemaFields(Map<String, Map<String, Object>> fields, SolrClient client) throws Exception {
        for (String name : fields.keySet()) {
            Map<String, Object> field = fields.get(name);
            SchemaRequest.AddField addField = new SchemaRequest.AddField(field);
            SchemaResponse.UpdateResponse r = addField.process(client);
            if (DefaultLogger.isTraceEnabled()) {
                DefaultLogger.trace(r);
            }
        }
    }

    private void updateSchemaFields(Map<String, Map<String, Object>> fields, SolrClient client) throws Exception {
        for (String name : fields.keySet()) {
            Map<String, Object> field = fields.get(name);
            SchemaRequest.ReplaceField replaceField = new SchemaRequest.ReplaceField(field);
            SchemaResponse.UpdateResponse r = replaceField.process(client);
            if (DefaultLogger.isTraceEnabled()) {
                DefaultLogger.trace(r);
            }
        }
    }

    private void scanType(Class<?> type,
                          SchemaScanResponse response,
                          Map<String, Map<String, Object>> schema) throws Exception {
        response.scanned.add(type);
        Field[] fields = ReflectionHelper.getAllFields(type);
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(org.apache.solr.client.solrj.beans.Field.class)) {
                    org.apache.solr.client.solrj.beans.Field f =
                            field.getAnnotation(org.apache.solr.client.solrj.beans.Field.class);
                    SolrFieldTypes t = SolrFieldTypes.getType(field);
                    if (t != null) {
                        String name = f.value();
                        if (Strings.isNullOrEmpty(name)) {
                            name = field.getName();
                        }
                        if (schema.containsKey(name)) {
                            Map<String, Object> s = schema.get(name);
                            String st = (String) s.get(SchemaConstants.TYPE);
                            if (st.compareTo(t.type()) == 0) {
                                continue;
                            }
                            s.put(SchemaConstants.TYPE, t.type());
                            s.put(SchemaConstants.STORED, true);
                            s.put(SchemaConstants.INDEXED, true);
                            s.put(SchemaConstants.DOC_VALUES, true);
                            response.updated.put(name, s);
                        } else {
                            Map<String, Object> s = new HashMap<>();
                            s.put(SchemaConstants.NAME, name);
                            s.put(SchemaConstants.TYPE, t.type());
                            s.put(SchemaConstants.STORED, true);
                            s.put(SchemaConstants.INDEXED, true);
                            s.put(SchemaConstants.DOC_VALUES, true);
                            response.added.put(name, s);
                        }
                    } else {
                        DefaultLogger.warn(String.format("[SOLR SCHEMA] Field type not supported. [type=%s]",
                                field.getType().getCanonicalName()));
                    }
                } else if (!ReflectionHelper.isPrimitiveTypeOrString(field)
                        && !field.getType().isEnum()
                        && !ReflectionHelper.isSuperType(Throwable.class, field.getType())
                        && !field.getType().equals(Object.class)
                        && !response.scanned.contains(type)) {
                    scanType(field.getType(), response, schema);
                }
            }
        }
    }

    private static ContentStreamUpdateRequest getContentUpdateRequest(Document<?, ?, ?> entity) throws Exception {
        String url = SOLR_URL_UPDATE;
        String mime = entity.getMimeType();
        if (needsExtract(mime)) {
            url = SOLR_URL_EXTRACT;
        }
        ContentStreamUpdateRequest ur = new ContentStreamUpdateRequest(url);
        ur.addFile(entity.getPath(), entity.getMimeType());
        setDocumentFields(ur, entity);
        if (mime.compareToIgnoreCase(FileUtils.MIME_TYPE_JSON) == 0)
            ur.setParam("json.command", "false");
        if (mime.compareToIgnoreCase(FileUtils.MIME_TYPE_XML) == 0)
            ur.setParam("xml.command", "false");
        String json = JSONUtils.asString(entity, entity.getClass());
        ur.setParam(LITERALS_PREFIX + SolrConstants.FIELD_SOLR_JSON_DATA, json);
        if (entity.getProperties() != null) {
            Map<String, Object> properties = entity.getProperties();
            for (String key : properties.keySet()) {
                Object prop = properties.get(key);
                if (ReflectionHelper.isPrimitiveTypeOrString(prop.getClass())) {
                    ur.setParam(LITERALS_PREFIX + key, String.valueOf(prop));
                } else if (prop.getClass().isEnum()) {
                    ur.setParam(LITERALS_PREFIX + key, ((Enum<?>) prop).name());
                } else if (prop instanceof Date) {
                    ur.setParam(LITERALS_PREFIX + key, prop.toString());
                }
            }
        }
        ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        return ur;
    }

    private static void setDocumentFields(ContentStreamUpdateRequest request, Document<?, ?, ?> document) throws Exception {
        Field[] fields = ReflectionHelper.getAllFields(document.getClass());
        Preconditions.checkNotNull(fields);
        for (Field field : fields) {
            if (field.isAnnotationPresent(org.apache.solr.client.solrj.beans.Field.class)) {
                org.apache.solr.client.solrj.beans.Field f =
                        field.getAnnotation(org.apache.solr.client.solrj.beans.Field.class);
                String name = f.value();
                if (Strings.isNullOrEmpty(name)) {
                    name = field.getName();
                }
                Object value = ReflectionHelper.getFieldValue(document, field, true);
                if (value != null) {
                    request.setParam(LITERALS_PREFIX + name, String.valueOf(value));
                }
            }
        }
    }

    private static boolean needsExtract(String mimeType) {
        if (FileUtils.isArchiveType(mimeType)
                || mimeType.compareToIgnoreCase(FileUtils.MIME_TYPE_PDF) == 0
                || FileUtils.isOfficeFileType(mimeType)) {
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               Context context) throws DataStoreException {
        checkState();
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = getCollection(key, type);
            SolrClient client = connection.connect(cname);
            String k = null;
            Q luceneQuery = null;
            if (key instanceof String) {
                k = (String) key;
                luceneQuery = EntityQueryBuilder.build(type, k);
            } else if (key instanceof DocumentId) {
                k = ((DocumentId) key).getId();
                luceneQuery = DocumentQueryBuilder.build((Class<? extends Document<?, ?, ?>>) type,
                        cname,
                        (DocumentId) key);
            } else if (key instanceof IKey) {
                k = ((IKey) key).stringKey();
                luceneQuery = EntityQueryBuilder.build(type, (IKey) key);
            } else {
                throw new DataStoreException(String.format("Key type not supported. [type=%s]",
                        key.getClass().getCanonicalName()));
            }
            SolrQuery q = new SolrQuery(luceneQuery.where());
            if (ReflectionHelper.isSuperType(SolrEntity.class, type)) {
                final QueryResponse response = client.query(q);
                List<E> entities = (List<E>) response.getBeans(type);
                if (entities != null) {
                    if (entities.size() > 1) {
                        throw new DataStoreException(
                                String.format("Multiple entries found for key. [type=%s][key=%s]",
                                        type.getCanonicalName(), k));
                    }
                    if (!entities.isEmpty()) {
                        SolrEntity<?> entity = (SolrEntity<?>) entities.get(0);
                        entity.getState().setState(EEntityState.Synced);
                        return (E) entity;
                    }
                }
            } else if (ReflectionHelper.isSuperType(Document.class, type)) {
                boolean fetchChildren = true;
                if (context instanceof SearchContext) {
                    fetchChildren = ((SearchContext) context).fetchChildDocuments();
                }
                final QueryResponse response = client.query(q);
                SolrDocumentList documents = response.getResults();
                if (documents != null && !documents.isEmpty()) {
                    if (documents.size() > 1) {
                        throw new DataStoreException(
                                String.format("Multiple entries found for key. [type=%s][key=%s]",
                                        type.getCanonicalName(), k));
                    }
                    SolrDocument doc = documents.get(0);
                    Document<?, ?, ?> document = readDocument(doc,
                            k,
                            (Class<? extends Document<?, ?, ?>>) type,
                            fetchChildren);
                    return (E) document;
                }
            } else {
                final QueryResponse response = client.query(q);
                List<SolrJsonEntity> entities = (List<SolrJsonEntity>) response.getBeans(SolrJsonEntity.class);
                if (entities != null) {
                    if (entities.size() > 1) {
                        throw new DataStoreException(
                                String.format("Multiple entries found for key. [type=%s][key=%s]",
                                        type.getCanonicalName(), k));
                    }
                    if (!entities.isEmpty()) {
                        SolrJsonEntity json = entities.get(0);
                        return JSONUtils.read(json.getJson(), type);
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    public Document<?, ?, ?> readDocument(@NonNull SolrDocument doc,
                                          @NonNull String key,
                                          @NonNull Class<? extends Document<?, ?, ?>> type,
                                          boolean fetchChildren) throws Exception {
        Object fv = doc.getFieldValue(SolrConstants.FIELD_SOLR_JSON_DATA);
        if (fv == null) {
            throw new DataStoreException(
                    String.format("Search returned NULL object for key. [type=%s][key=%s]",
                            type.getCanonicalName(), key));
        }
        String json = null;
        if (fv instanceof String) {
            json = (String) fv;
        } else if (fv instanceof Collection<?>) {
            Collection<?> c = (Collection<?>) fv;
            if (c.isEmpty()) {
                throw new DataStoreException(
                        String.format("Search returned empty array for key. [type=%s][key=%s]",
                                type.getCanonicalName(), key));
            }
            for (Object o : c) {
                if (o instanceof String) {
                    json = (String) o;
                }
            }
        }
        if (Strings.isNullOrEmpty(json)) {
            throw new DataStoreException(
                    String.format("Search returned empty json for key. [type=%s][key=%s]",
                            type.getCanonicalName(), key));
        }
        Document<?, ?, ?> document = JSONUtils.read(json, type);
        if (document.getDocumentCount() > 0 && fetchChildren) {
            fetchChildren(document, true);
        }
        return document;
    }

    @SuppressWarnings("unchecked")
    public void fetchChildren(@NonNull Document<?, ?, ?> parent,
                              boolean fetchChildren) throws Exception {
        if (parent.getDocumentCount() <= 0) return;
        Set<Document<?, ?, ?>> children = fetchChildren(parent.getId(),
                (Class<? extends Document<?, ?, ?>>) parent.getClass(),
                fetchChildren);
        if (children != null && !children.isEmpty()) {
            for (Document<?, ?, ?> document : children) {
                parent.add(document);
            }
        }
    }

    public Set<Document<?, ?, ?>> fetchChildren(@NonNull DocumentId parent,
                                                @NonNull Class<? extends Document<?, ?, ?>> type,
                                                boolean fetchChildren) throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder(Document.class,
                parent.getCollection());
        builder.createPhraseQuery(SolrConstants.FIELD_DOC_PARENT_ID, parent.getId());
        try (Cursor<DocumentId, Document<?, ?, ?>> cursor = search(builder.build(),
                DocumentId.class,
                type,
                null)) {
            Set<Document<?, ?, ?>> documents = new HashSet<>();
            while (true) {
                List<Document<?, ?, ?>> docs = cursor.nextPage();
                if (docs == null || docs.isEmpty()) break;
                for (Document<?, ?, ?> doc : docs) {
                    if (doc.getDocumentCount() > 0 && fetchChildren) {
                        fetchChildren(doc, true);
                    }
                    documents.add(doc);
                }
            }
            if (!documents.isEmpty()) {
                return documents;
            }
        }
        return null;
    }

    @Override
    public <K extends IKey, E extends IEntity<K>> Cursor<K, E> doSearch(@NonNull Q query,
                                                                        int maxResults,
                                                                        @NonNull Class<? extends K> keyType,
                                                                        @NonNull Class<? extends E> type,
                                                                        Context context) throws DataStoreException {
        if (!(query instanceof EntityQueryBuilder.LuceneQuery)) {
            throw new DataStoreException(String.format("Invalid Query type. [type=%s]",
                    query.getClass().getCanonicalName()));
        }
        try {
            SolrConnection connection = (SolrConnection) connection();
            String cname = ((EntityQueryBuilder.LuceneQuery) query).collection();
            if (Strings.isNullOrEmpty(cname)) {
                cname = getCollection(type);
            }
            boolean fetchChildren = true;
            if (context instanceof SearchContext) {
                fetchChildren = ((SearchContext) context).fetchChildDocuments();
            }
            SolrClient client = connection.connect(cname);
            EntityQueryBuilder.LuceneQuery q = (EntityQueryBuilder.LuceneQuery) query;
            return new SolrCursor<>(type, this, client, q, maxResults, fetchChildren);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
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
