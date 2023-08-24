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

package ai.sapper.cdc.core.stores.impl;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.model.entity.EEntityState;
import ai.sapper.cdc.common.model.entity.IEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.model.BaseEntity;
import ai.sapper.cdc.core.stores.BaseSearchResult;
import ai.sapper.cdc.core.stores.DataStoreException;
import ai.sapper.cdc.core.stores.TransactionDataStore;
import ai.sapper.cdc.core.stores.impl.settings.MongoDbSettings;
import com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryResultIterator;
import com.google.common.base.Preconditions;
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
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.persistence.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDbDataStore extends TransactionDataStore<ClientSession, MongoTransaction> {
    public static final String FIELD_DOC_ID = "_id";

    private MongoDatabase database;

    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkState(settings instanceof MongoDbSettings);
        try {
            MongoDSConnection connection = (MongoDSConnection) connection();
            if (!connection.isConnected()) {
                connection.connect();
            }
            sessionManager(new MongoSessionManager(connection, ((MongoDbSettings) settings).getSessionTimeout()));
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
            String cname = getCollection(entity);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
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
            String cname = getCollection(entity);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
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
            String cname = getCollection(type);
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

    @SuppressWarnings("unchecked")
    private String getCollection(IEntity<?> entity) {
        return getCollection((Class<? extends IEntity<?>>) entity.getClass());
    }

    private String getCollection(Class<? extends IEntity<?>> type) {
        String name = type.getSimpleName();
        if (type.isAnnotationPresent(Table.class)) {
            Table table = type.getAnnotation(Table.class);
            name = table.name();
        }
        return name;
    }
}
