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
import dev.morphia.annotations.Entity;
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
        Preconditions.checkArgument(entity instanceof MongoEntity<?>);
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        try {
            ClientSession session = sessionManager().session();
            String cname = getCollection(entity.getClass());
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            ((MongoEntity<?>) entity).set_id(entity.getKey().stringKey());
            ((MongoEntity<?>) entity).set_type(entity.getClass().getCanonicalName());
            ((MongoEntity<?>) entity).setCreatedTime(System.nanoTime());
            ((MongoEntity<?>) entity).setUpdatedTime(System.nanoTime());
            MongoCollection<Document> collection = database.getCollection(cname);
            String json = JSONUtils.asString(entity, type);
            Document doc = Document.parse(json);
            doc.put(JsonFieldConstants.FIELD_DOC_ID, entity.getKey().stringKey());
            doc.put(JsonFieldConstants.FIELD_DOC_TYPE, entity.getClass().getCanonicalName());
            InsertOneResult r = collection.insertOne(session, doc);
            ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
            return entity;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
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
        Preconditions.checkArgument(entity instanceof MongoEntity<?>);
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        try {
            ClientSession session = sessionManager().session();
            String cname = getCollection(entity.getClass());
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            ((MongoEntity<?>) entity).setUpdatedTime(System.nanoTime());
            MongoCollection<Document> collection = database.getCollection(cname);
            String json = JSONUtils.asString(entity, type);
            Document doc = Document.parse(json);
            doc.put(JsonFieldConstants.FIELD_DOC_ID, entity.getKey().stringKey());
            doc.put(JsonFieldConstants.FIELD_DOC_TYPE, entity.getClass().getCanonicalName());
            Bson filter = Filters.eq(JsonFieldConstants.FIELD_DOC_ID, entity.getKey().stringKey());
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
            ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
            return entity;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    private String getCollection(Class<?> type) throws Exception {
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
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws DataStoreException {
        checkState();
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        Preconditions.checkArgument(key instanceof String);
        try {
            ClientSession session = sessionManager().session();
            String cname = getCollection(type);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            Bson filter = Filters.eq(JsonFieldConstants.FIELD_DOC_ID, key);
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
            String cname = getCollection(type);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            Bson filter = Filters.eq(JsonFieldConstants.FIELD_DOC_ID, key);
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
            String cname = getCollection(type);
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
            String cname = getCollection(type);
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
