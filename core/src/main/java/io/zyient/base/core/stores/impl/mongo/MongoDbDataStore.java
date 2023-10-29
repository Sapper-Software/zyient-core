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

package io.zyient.base.core.stores.impl.mongo;

import com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryResultIterator;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.stores.*;
import io.zyient.base.core.stores.annotations.Reference;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.stores.impl.settings.mongo.MongoDbSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.persistence.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MongoDbDataStore extends TransactionDataStore<ClientSession, MongoTransaction> {
    public static final String FIELD_DOC_ID = "_id";

    private MongoDatabase database;

    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkState(settings instanceof MongoDbSettings);
        try {
            MongoDbConnection connection = (MongoDbConnection) connection();
            if (!connection.isConnected()) {
                connection.connect();
            }
            sessionManager(new MongoSessionManager(connection,
                    ((MongoDbSettings) settings).getSessionTimeout().normalized()));
            database = connection.getConnection().getDatabase(((MongoDbSettings) settings).getDb());
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public static TransactionOptions getTransactionOptions() {
        return TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();
    }

    @Override
    public <E extends IEntity<?>> E createEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        try {
            ClientSession session = sessionManager().session();
            String cname = EntityUtils.getCollection(entity);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            entity = checkAndCreateReference(entity, type, context, session);
            String json = JSONUtils.asString(entity, type);
            Document doc = Document.parse(json);
            doc.put(FIELD_DOC_ID, entity.getKey().stringKey());
            InsertOneResult r = collection.insertOne(session, doc);
            if (entity instanceof BaseEntity<?>) {
                ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
            }
            return entity;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity<?>> E checkAndCreateReference(E entity,
                                                             Class<? extends E> type,
                                                             Context context,
                                                             ClientSession session) throws Exception {
        Field[] fields = ReflectionUtils.getAllFields(type);
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Reference.class)) {
                    Reference ref = field.getAnnotation(Reference.class);
                    String tf = ref.reference();
                    if (!Strings.isNullOrEmpty(tf)) {
                        Object value = ReflectionUtils.getFieldValue(entity, field, true);
                        if (value == null) continue;
                        if (!(value instanceof IEntity<?>)) {
                            throw new Exception(
                                    String.format("Invalid embedded field. [field=%s][type=%s]",
                                            field.getName(), field.getType().getCanonicalName()));
                        }
                        Field rfield = ReflectionUtils.findField(type, tf);
                        if (rfield == null) {
                            throw new DataStoreException(
                                    String.format("Reference field not found. [field=%s][type=%s]",
                                            tf, type.getCanonicalName()));
                        }
                        if (!ReflectionUtils.isSuperType(JsonReference.class, rfield.getType())) {
                            throw new DataStoreException(
                                    String.format("Invalid reference field type. [type=%s][class=%s]",
                                            rfield.getType().getCanonicalName(), type.getCanonicalName()));
                        }
                        Class<? extends IEntity<?>> rtype = ref.target();
                        entity = nestedUpdates(entity,
                                type,
                                context,
                                session,
                                field,
                                value,
                                rfield,
                                new EEntityState[]{EEntityState.New},
                                rtype);
                    }
                }
            }
        }
        return entity;
    }

    private <E extends IEntity<?>> E nestedUpdates(E entity,
                                                   Class<? extends E> type,
                                                   Context context,
                                                   ClientSession session,
                                                   Field sourceField,
                                                   Object sourceValue,
                                                   Field referenceField,
                                                   EEntityState[] states,
                                                   Class<? extends IEntity<?>> referenceType) throws Exception {
        JsonReference ref = (JsonReference) ReflectionUtils.getFieldValue(entity, referenceField, true);
        if (ref == null) {
            ref = new JsonReference();
        }
        if (sourceField.isAnnotationPresent(OneToMany.class)) {
            OneToMany o2m = sourceField.getAnnotation(OneToMany.class);
            CascadeType[] cascadeTypes = o2m.cascade();
            if (ReflectionUtils.implementsInterface(Collection.class, sourceField.getType())) {
                handleCollectionCreate(referenceType,
                        context,
                        session,
                        sourceField,
                        sourceValue,
                        referenceField,
                        cascadeTypes,
                        states,
                        ref);
            } else if (ReflectionUtils.isSuperType(Map.class, sourceField.getType())) {
                if (!sourceField.isAnnotationPresent(MapKey.class)) {
                    throw new Exception(
                            String.format("Map Key not found. [field=%s][class=%s]",
                                    sourceField.getName(), type.getCanonicalName()));
                }
                handleMapCreate(referenceType,
                        context,
                        session,
                        sourceField,
                        sourceValue,
                        referenceField,
                        cascadeTypes,
                        states,
                        ref);
            } else {
                throw new Exception(
                        String.format("Unsupported field type. [type=%s][class=%s]",
                                sourceField.getType().getCanonicalName(), type.getCanonicalName()));
            }
        } else if (sourceField.isAnnotationPresent(ManyToMany.class)) {
            ManyToMany m2m = sourceField.getAnnotation(ManyToMany.class);
            CascadeType[] cascadeTypes = m2m.cascade();
            if (ReflectionUtils.implementsInterface(Collection.class, sourceField.getType())) {
                handleCollectionCreate(referenceType,
                        context,
                        session,
                        sourceField,
                        sourceValue,
                        referenceField,
                        cascadeTypes,
                        states,
                        ref);
            } else if (ReflectionUtils.isSuperType(Map.class, sourceField.getType())) {
                if (!sourceField.isAnnotationPresent(MapKey.class)) {
                    throw new Exception(
                            String.format("Map Key not found. [field=%s][class=%s]",
                                    sourceField.getName(), type.getCanonicalName()));
                }
                handleMapCreate(referenceType,
                        context,
                        session,
                        sourceField,
                        sourceValue,
                        referenceField,
                        cascadeTypes,
                        states,
                        ref);
            } else {
                throw new Exception(
                        String.format("Unsupported field type. [type=%s][class=%s]",
                                sourceField.getType().getCanonicalName(), type.getCanonicalName()));
            }
        } else if (sourceField.isAnnotationPresent(OneToOne.class)) {
            OneToOne o2o = sourceField.getAnnotation(OneToOne.class);
            IEntity<?> ie = (IEntity<?>) sourceValue;
            ref.add(ie, EEntityState.New);
            CascadeType[] cascadeTypes = o2o.cascade();
            if (hasCascadeType(CascadeType.PERSIST, cascadeTypes)) {
                boolean create = true;
                if (ie instanceof BaseEntity<?>) {
                    if (((BaseEntity<?>) ie).getState().getState() != EEntityState.New) {
                        create = false;
                    }
                }
                if (create) {
                    create(ie, ie.getClass(), context);
                }
            }
        }
        ReflectionUtils.setValue(ref, entity, referenceField);
        return entity;
    }

    @SuppressWarnings("unchecked")
    private <E extends IEntity<?>> void handleCollectionCreate(Class<? extends E> type,
                                                               Context context,
                                                               ClientSession session,
                                                               Field sourceField,
                                                               Object sourceValue,
                                                               Field referenceField,
                                                               CascadeType[] cascadeTypes,
                                                               EEntityState[] states,
                                                               JsonReference ref) throws Exception {
        Collection<E> values = (Collection<E>) sourceValue;
        ref.addAll(values);
        if (hasCascadeType(CascadeType.PERSIST, cascadeTypes)) {
            for (E value : values) {
                if (value instanceof BaseEntity<?>) {
                    if (!((BaseEntity<?>) value).getState().inState(states)) {
                        continue;
                    }
                }
                create(value, value.getClass(), context);
            }
        }
    }

    private boolean hasCascadeType(CascadeType type, CascadeType[] values) {
        for (CascadeType value : values) {
            if (type == value || value == CascadeType.ALL)
                return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private <E extends IEntity<?>> void handleMapCreate(Class<? extends E> type,
                                                        Context context,
                                                        ClientSession session,
                                                        Field sourceField,
                                                        Object sourceValue,
                                                        Field referenceField,
                                                        CascadeType[] cascadeTypes,
                                                        EEntityState[] states,
                                                        JsonReference ref) throws Exception {
        Map<?, E> values = (Map<?, E>) sourceValue;
        ref.addAll(values);
        if (hasCascadeType(CascadeType.PERSIST, cascadeTypes)) {
            for (Object k : values.keySet()) {
                E value = values.get(k);
                if (value instanceof BaseEntity<?>) {
                    if (!((BaseEntity<?>) value).getState().inState(states)) {
                        continue;
                    }
                }
                create(value, value.getClass(), context);
            }
        }
    }

    private boolean collectionExists(String name) {
        try (MongoCursor<String> cursor = database.listCollectionNames().cursor()) {
            while (cursor.hasNext()) {
                String n = cursor.next();
                if (n.compareTo(name) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        try {
            ClientSession session = sessionManager().session();
            String cname = EntityUtils.getCollection(entity);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            entity = checkAndUpdateReference(entity, type, context, session);
            String json = JSONUtils.asString(entity, type);
            Document doc = Document.parse(json);
            doc.put(FIELD_DOC_ID, entity.getKey().stringKey());
            Bson filter = Filters.eq(FIELD_DOC_ID, entity.getKey().stringKey());
            UpdateResult r = collection.updateOne(filter, doc);
            if (r.getMatchedCount() == 0) {
                throw new DataStoreException(
                        String.format("Entity not found. [kaye=%s][type=%s]",
                                entity.getKey().stringKey(), type.getCanonicalName()));
            } else if (r.getMatchedCount() > 1) {
                throw new DataStoreException(
                        String.format("Entity not found. [kaye=%s][type=%s]",
                                entity.getKey().stringKey(), type.getCanonicalName()));
            }
            if (entity instanceof BaseEntity<?>) {
                ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
            }
            return entity;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    private <E extends IEntity<?>> E checkAndUpdateReference(E entity,
                                                             Class<? extends E> type,
                                                             Context context,
                                                             ClientSession session) throws Exception {
        Field[] fields = ReflectionUtils.getAllFields(type);
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Reference.class)) {
                    Reference ref = field.getAnnotation(Reference.class);
                    String tf = ref.reference();
                    if (!Strings.isNullOrEmpty(tf)) {
                        Object value = ReflectionUtils.getFieldValue(entity, field, true);
                        if (value == null) continue;
                        if (!(value instanceof IEntity<?>)) {
                            throw new Exception(
                                    String.format("Invalid embedded field. [field=%s][type=%s]",
                                            field.getName(), field.getType().getCanonicalName()));
                        }
                        Field rfield = ReflectionUtils.findField(type, tf);
                        if (rfield == null) {
                            throw new DataStoreException(
                                    String.format("Reference field not found. [field=%s][type=%s]",
                                            tf, type.getCanonicalName()));
                        }
                        if (!ReflectionUtils.isSuperType(JsonReference.class, rfield.getType())) {
                            throw new DataStoreException(
                                    String.format("Invalid reference field type. [type=%s][class=%s]",
                                            rfield.getType().getCanonicalName(), type.getCanonicalName()));
                        }
                        Class<? extends IEntity<?>> rtype = ref.target();
                        entity = nestedUpdates(entity,
                                type,
                                context,
                                session,
                                field,
                                value,
                                rfield,
                                new EEntityState[]{EEntityState.New, EEntityState.Updated},
                                rtype);
                    }
                }
            }
        }
        return entity;
    }

    @Override
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        Preconditions.checkArgument(key instanceof String);
        try {
            ClientSession session = sessionManager().session();
            String cname = EntityUtils.getCollection(type);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            Bson filter = Filters.eq(FIELD_DOC_ID, key);
            DeleteResult r = collection.deleteOne(filter);
            return (r.getDeletedCount() == 1);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        Preconditions.checkArgument(key instanceof String);
        try {
            ClientSession session = sessionManager().session();
            String cname = EntityUtils.getCollection(type);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            Bson filter = Filters.eq(FIELD_DOC_ID, key);
            FindIterable<Document> documents = collection.find(filter);
            for (Document document : documents) {
                String json = document.toJson();
                return JSONUtils.read(json, type);
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               @NonNull Class<? extends E> type,
                                                               Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        try {
            ClientSession session = sessionManager().session();
            String cname = EntityUtils.getCollection(type);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString(query).build();
            MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
            if (cname.compareTo(mongoDBQueryHolder.getCollection()) != 0) {
                throw new DataStoreException(
                        String.format("Query does not match entity collection. [expected=%s][query=%s]",
                                cname, mongoDBQueryHolder.getCollection()));
            }
            mongoDBQueryHolder.setOffset(offset);
            mongoDBQueryHolder.setLimit(maxResults);
            try (QueryResultIterator<Document> distinctIterable = queryConverter.run(database)) {
                List<Document> documents = Lists.newArrayList(distinctIterable);
                List<E> entities = new ArrayList<>();
                for (Document document : documents) {
                    String json = document.toJson();
                    E entity = JSONUtils.read(json, type);
                    if (entity instanceof BaseEntity) {
                        ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
                    }
                    entities.add(entity);
                }
                if (!entities.isEmpty()) {
                    EntitySearchResult<E> er = new EntitySearchResult<>(type);
                    er.setQuery(query);
                    er.setOffset(offset);
                    er.setCount(entities.size());
                    er.setEntities(entities);
                    return er;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               Map<String, Object> parameters,
                                                               @NonNull Class<? extends E> type,
                                                               Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        try {
            ClientSession session = sessionManager().session();
            String cname = EntityUtils.getCollection(type);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString(query).build();
            MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
            if (cname.compareTo(mongoDBQueryHolder.getCollection()) != 0) {
                throw new DataStoreException(
                        String.format("Query does not match entity collection. [expected=%s][query=%s]",
                                cname, mongoDBQueryHolder.getCollection()));
            }
            mongoDBQueryHolder.setOffset(offset);
            mongoDBQueryHolder.setLimit(maxResults);
            if (parameters != null && !parameters.isEmpty()) {
                Document qd = mongoDBQueryHolder.getQuery();
                for (String key : parameters.keySet()) {
                    Object v = parameters.get(key);
                    qd.put(key, v);
                }
            }
            try (QueryResultIterator<Document> distinctIterable = queryConverter.run(database)) {
                List<Document> documents = Lists.newArrayList(distinctIterable);
                List<E> entities = new ArrayList<>();
                for (Document document : documents) {
                    String json = document.toJson();
                    E entity = JSONUtils.read(json, type);
                    if (entity instanceof BaseEntity) {
                        ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
                    }
                    entities.add(entity);
                }
                if (!entities.isEmpty()) {
                    EntitySearchResult<E> er = new EntitySearchResult<>(type);
                    er.setQuery(query);
                    er.setOffset(offset);
                    er.setCount(entities.size());
                    er.setEntities(entities);
                    return er;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
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


    @Override
    public boolean isThreadSafe() {
        return true;
    }
}
