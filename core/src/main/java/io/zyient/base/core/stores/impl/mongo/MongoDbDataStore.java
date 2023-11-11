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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import dev.morphia.DeleteOptions;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Reference;
import dev.morphia.query.MorphiaCursor;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import dev.morphia.transactions.MorphiaSession;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.stores.*;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.stores.impl.settings.mongo.MongoDbSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MongoDbDataStore extends TransactionDataStore<MorphiaSession, MongoTransaction> {

    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkState(settings instanceof MongoDbSettings);
        try {
            MorphiaConnection connection = (MorphiaConnection) connection();
            if (!connection.isConnected()) {
                connection.connect();
            }
            sessionManager(new MongoSessionManager(connection,
                    ((MongoDbSettings) settings).getSessionTimeout().normalized()));
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
        Preconditions.checkState(isInTransaction());
        try {
            MorphiaSession session = sessionManager().session();
            entity.validate();
            if (entity instanceof MongoEntity<?>) {
                ((MongoEntity<?>) entity).preSave();
                checkReferences(entity, context);
                entity = session.save(entity);
                ((MongoEntity<?>) entity).getState().setState(EEntityState.Synced);
            } else {
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).setCreatedTime(System.nanoTime());
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }
                String json = JSONUtils.asString(entity, entity.getClass());
                Document doc = Document.parse(json);
                doc.put(JsonFieldConstants.FIELD_DOC_ID, entity.entityKey().stringKey());
                doc.put(JsonFieldConstants.FIELD_DOC_TYPE, type.getCanonicalName());
                if (!(entity instanceof BaseEntity<?>)) {
                    doc.put(JsonFieldConstants.FIELD_DOC_CREATED, System.nanoTime());
                    doc.put(JsonFieldConstants.FIELD_DOC_LAST_UPDATED, System.nanoTime());
                }
                String cname = getCollection(type);
                MongoDatabase db = session.getDatabase();
                InsertOneResult ir = db.getCollection(cname)
                        .insertOne(doc);
                if (!ir.wasAcknowledged()) {
                    throw new DataStoreException(String.format("Insert not acknowledged. [type=%s][id=%s]",
                            type.getCanonicalName(), entity.entityKey().stringKey()));
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

    private <E extends IEntity<?>> E checkReferences(E entity,
                                                     Context context) throws DataStoreException {
        try {
            Field[] fields = ReflectionUtils.getAllFields(entity.getClass());
            if (fields != null) {
                for (Field field : fields) {
                    if (field.isAnnotationPresent(Reference.class)) {
                        Class<?> type = field.getType();
                        if (type.isArray()) {
                            Class<?> inner = type.getComponentType();
                            if (!ReflectionUtils.isSuperType(MongoEntity.class, inner)) {
                                throw new DataStoreException(String.format("Array type not supported. [type=%s]",
                                        inner.getCanonicalName()));
                            }
                            Object value = ReflectionUtils.getFieldValue(entity, field);
                            if (value != null) {
                                Object[] array = ReflectionUtils.convertToObjectArray(value);
                                for (Object av : array) {
                                    MongoEntity<?> me = (MongoEntity<?>) av;
                                    if (me.getState().getState() == EEntityState.New) {
                                        createEntity(me, me.getClass(), context);
                                    } else {
                                        me.preSave();
                                        if (me.getState().getState() == EEntityState.New) {
                                            createEntity(me, me.getClass(), context);
                                        } else if (me.getState().getState() == EEntityState.Updated) {
                                            updateEntity(me, me.getClass(), context);
                                        }
                                    }
                                }
                            }
                        } else if (ReflectionUtils.isCollection(type)) {
                            Class<?> inner = ReflectionUtils.getGenericCollectionType(field);
                            if (!ReflectionUtils.isSuperType(MongoEntity.class, inner)) {
                                throw new DataStoreException(String.format("Collection type not supported. [type=%s]",
                                        inner.getCanonicalName()));
                            }
                            Object value = ReflectionUtils.getFieldValue(entity, field);
                            if (value != null) {
                                Collection<?> collection = (Collection<?>) value;

                                for (Object av : collection) {
                                    MongoEntity<?> me = (MongoEntity<?>) av;
                                    if (me.getState().getState() == EEntityState.New) {
                                        createEntity(me, me.getClass(), context);
                                    } else {
                                        me.preSave();
                                        if (me.getState().getState() == EEntityState.New) {
                                            createEntity(me, me.getClass(), context);
                                        } else if (me.getState().getState() == EEntityState.Updated) {
                                            updateEntity(me, me.getClass(), context);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return entity;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        try {
            MorphiaSession session = sessionManager().session();
            entity.validate();
            if (entity instanceof MongoEntity<?>) {
                ((MongoEntity<?>) entity).preSave();
                Query<E> query = (Query<E>) session.find(type)
                        .filter(Filters.eq(JsonFieldConstants.FIELD_DOC_ID, entity.entityKey().stringKey()));
                try (MorphiaCursor<E> cursor = query.iterator()) {
                    List<E> found = cursor.toList();
                    if (found.size() != 1) {
                        throw new DataStoreException(String.format("Multiple records found for key. [type=%s][key=%s]",
                                type.getCanonicalName(), entity.entityKey().stringKey()));
                    }
                    entity = session.save(entity);
                    ((MongoEntity<?>) entity).getState().setState(EEntityState.Synced);
                }
            } else {
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).setCreatedTime(System.nanoTime());
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }
                String json = JSONUtils.asString(entity, entity.getClass());
                Document doc = Document.parse(json);
                doc.put(JsonFieldConstants.FIELD_DOC_ID, entity.entityKey().stringKey());
                doc.put(JsonFieldConstants.FIELD_DOC_TYPE, type.getCanonicalName());
                if (!(entity instanceof BaseEntity<?>)) {
                    doc.put(JsonFieldConstants.FIELD_DOC_CREATED, System.nanoTime());
                    doc.put(JsonFieldConstants.FIELD_DOC_LAST_UPDATED, System.nanoTime());
                }
                String cname = getCollection(type);
                MongoDatabase db = session.getDatabase();
                Bson filter = com.mongodb.client.model.Filters.eq(JsonFieldConstants.FIELD_DOC_ID,
                        entity.entityKey().stringKey());
                UpdateResult ur = db.getCollection(cname)
                        .updateOne(filter, doc);
                if (!ur.wasAcknowledged()) {
                    throw new DataStoreException(String.format("Insert not acknowledged. [type=%s][id=%s]",
                            type.getCanonicalName(), entity.entityKey().stringKey()));
                }
                if (ur.getMatchedCount() == 0) {
                    throw new DataStoreException(
                            String.format("Entity not found. [kaye=%s][type=%s]",
                                    entity.entityKey().stringKey(), type.getCanonicalName()));
                } else if (ur.getMatchedCount() > 1) {
                    throw new DataStoreException(
                            String.format("Entity not found. [kaye=%s][type=%s]",
                                    entity.entityKey().stringKey(), type.getCanonicalName()));
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
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        Preconditions.checkArgument(key instanceof String || key instanceof IKey);
        try {
            String k = null;
            if (key instanceof String) {
                k = (String) key;
            } else {
                k = ((IKey) key).stringKey();
            }
            MorphiaSession session = sessionManager().session();
            if (ReflectionUtils.isSuperType(MongoEntity.class, type)) {
                Query<E> query = (Query<E>) session.find(type)
                        .filter(Filters.eq(JsonFieldConstants.FIELD_DOC_ID, k));
                try (MorphiaCursor<E> cursor = query.iterator()) {
                    List<E> found = cursor.toList();
                    if (found.size() != 1) {
                        throw new DataStoreException(String.format("Multiple records found for key. [type=%s][key=%s]",
                                type.getCanonicalName(), k));
                    }
                    DeleteResult dr = query.delete(new DeleteOptions().multi(false));
                    if (dr.getDeletedCount() != 1) {
                        return false;
                    }
                }
            } else {
                MongoDatabase db = session.getDatabase();
                String cname = getCollection(type);
                Bson filter = com.mongodb.client.model.Filters.eq(JsonFieldConstants.FIELD_DOC_ID, k);
                DeleteResult dr = db.getCollection(cname)
                        .deleteOne(filter);
                return (dr.getDeletedCount() == 1);
            }
            return true;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               Context context) throws DataStoreException {
        checkState();
        Preconditions.checkArgument(key instanceof String || key instanceof IKey);
        try {
            MorphiaSession session = sessionManager().session();
            String k = null;
            if (key instanceof String) {
                k = (String) key;
            } else {
                k = ((IKey) key).stringKey();
            }
            if (ReflectionUtils.isSuperType(MongoEntity.class, type)) {
                Query<E> query = (Query<E>) session.find(type)
                        .filter(Filters.eq(JsonFieldConstants.FIELD_DOC_ID, k));
                try (MorphiaCursor<E> cursor = query.iterator()) {
                    List<E> found = cursor.toList();
                    if (!found.isEmpty()) {
                        if (found.size() > 1) {
                            throw new DataStoreException(String.format("Multiple records found for key. [type=%s][key=%s]",
                                    type.getCanonicalName(), k));
                        }
                        MongoEntity<?> me = (MongoEntity<?>) found.get(0);
                        me.postLoad();
                        return (E) me;
                    }
                }
            } else {
                String cname = getCollection(type);
                MongoCollection<Document> collection = session.getDatabase().getCollection(cname);
                Bson filter = com.mongodb.client.model.Filters.eq(JsonFieldConstants.FIELD_DOC_ID, k);
                FindIterable<Document> documents = collection.find(filter);
                for (Document document : documents) {
                    String json = document.toJson();
                    return JSONUtils.read(json, type);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private String getCollection(Class<?> type) {
        String name = null;
        if (type.isAnnotationPresent(Entity.class)) {
            Entity e = type.getAnnotation(Entity.class);
            name = e.value();
        }
        if (Strings.isNullOrEmpty(name)) {
            name = type.getSimpleName();
        }
        return name;
    }

    @Override
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               @NonNull Class<? extends E> type,
                                                               Context context) throws DataStoreException {
        checkState();
        try {
            MorphiaSession session = sessionManager().session();
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString(query).build();
            MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
            String cname = getCollection(type);
            if (Strings.isNullOrEmpty(cname) || cname.compareTo(mongoDBQueryHolder.getCollection()) != 0) {
                throw new DataStoreException(
                        String.format("Query does not match entity collection. [expected=%s][query=%s]",
                                cname, mongoDBQueryHolder.getCollection()));
            }
            mongoDBQueryHolder.setOffset(offset);
            mongoDBQueryHolder.setLimit(maxResults);
            try (QueryResultIterator<Document> distinctIterable = queryConverter.run(session.getDatabase())) {
                List<Document> documents = Lists.newArrayList(distinctIterable);
                List<E> entities = new ArrayList<>();
                for (Document document : documents) {
                    String json = document.toJson();
                    E entity = JSONUtils.read(json, type);
                    if (entity instanceof MongoEntity<?>) {
                        ((MongoEntity<?>) entity).postLoad();
                    } else if (entity instanceof BaseEntity<?>) {
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
        try {
            MorphiaSession session = sessionManager().session();
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString(query).build();
            MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
            String cname = getCollection(type);
            if (Strings.isNullOrEmpty(cname) || cname.compareTo(mongoDBQueryHolder.getCollection()) != 0) {
                throw new DataStoreException(
                        String.format("Query does not match entity collection. [expected=%s][query=%s]",
                                cname, mongoDBQueryHolder.getCollection()));
            }
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
            try (QueryResultIterator<Document> distinctIterable = queryConverter.run(session.getDatabase())) {
                List<Document> documents = Lists.newArrayList(distinctIterable);
                List<E> entities = new ArrayList<>();
                for (Document document : documents) {
                    String json = document.toJson();
                    E entity = JSONUtils.read(json, type);
                    if (entity instanceof MongoEntity<?>) {
                        ((MongoEntity<?>) entity).postLoad();
                    } else if (entity instanceof BaseEntity<?>) {
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
