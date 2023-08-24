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
import ai.sapper.cdc.common.model.entity.IEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.stores.AbstractDataStore;
import ai.sapper.cdc.core.stores.BaseSearchResult;
import ai.sapper.cdc.core.stores.DataStoreException;
import ai.sapper.cdc.core.stores.TransactionDataStore;
import ai.sapper.cdc.core.stores.impl.settings.MongoDbSettings;
import com.google.common.base.Preconditions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.Document;
import org.bson.json.JsonObject;

import javax.persistence.Table;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class MongoDbDataStore extends TransactionDataStore<MongoClient, ClientSession> {
    public static final String FIELD_DOC_ID = "_id";

    private MongoDatabase database;

    public MongoDbDataStore() {
        super(new MongoDbTransactionCache());
    }

    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkState(settings instanceof MongoDbSettings);
        try {
            MongoDSConnection connection = (MongoDSConnection) connection();
            if (!connection.isConnected()) {
                connection.connect();
            }
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
        Preconditions.checkNotNull(database);
        Preconditions.checkState(isInTransaction());
        try {
            ClientSession session = session();
            String cname = getCollection(entity);
            if (!collectionExists(cname)) {
                database.createCollection(cname);
            }
            MongoCollection<Document> collection = database.getCollection(cname);
            String json = JSONUtils.asString(entity, type);
            Document doc = Document.parse(json);
            doc.put(FIELD_DOC_ID, entity.getKey().stringKey());
            collection.insertOne(session, doc);
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
        DataStoreAuditContext ctx = new DataStoreAuditContext();
        ctx.setType(getClass().getCanonicalName());
        ctx.setName(name());
        ctx.setConnectionType(connection().getClass().getCanonicalName());
        ctx.setConnectionName(connection().name());
        return ctx;
    }

    private String getCollection(IEntity<?> entity) {
        String name = entity.getClass().getSimpleName();
        if (entity.getClass().isAnnotationPresent(Table.class)) {
            Table table = entity.getClass().getAnnotation(Table.class);
            name = table.name();
        }
        return name;
    }

    private ClientSession session() throws Exception {
        MongoDSConnection connection = (MongoDSConnection) connection();
        ClientSession session = transactions().get();
        if (session == null) {
            session = connection.getConnection().startSession();
            transactions().add(session);
        }
        return session;
    }

    @Override
    public boolean isInTransaction() throws DataStoreException {
        ClientSession session = transactions().get();
        if (session != null) {
            return session.hasActiveTransaction();
        }
        return false;
    }

    @Override
    public void beingTransaction() throws DataStoreException {
        try {
            ClientSession session = session();
            if (!session.hasActiveTransaction()) {
                session.startTransaction(getTransactionOptions());
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void commit() throws DataStoreException {
        try {
            transactions().commit();
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void rollback() throws DataStoreException {
        try {
            transactions().rollback();
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }
}
