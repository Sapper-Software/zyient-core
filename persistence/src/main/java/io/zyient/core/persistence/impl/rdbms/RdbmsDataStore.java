/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.persistence.impl.rdbms;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.persistence.*;
import io.zyient.core.persistence.annotations.EGeneratedType;
import io.zyient.core.persistence.impl.settings.rdbms.RdbmsStoreSettings;
import io.zyient.core.persistence.model.BaseEntity;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;

public class RdbmsDataStore extends TransactionDataStore<Session, Transaction> {
    public void flush() throws DataStoreException {
        checkState();
        Session session = sessionManager().session(false);
        if (session != null) {
            session.flush();
        }
    }

    @Override
    public <E extends IEntity<?>> E createEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session(false);
        if (entity instanceof BaseEntity) {
            ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
        }
        IDGenerator.process(entity, this);
        session.persist(entity);
        return entity;
    }

    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkState(settings instanceof RdbmsStoreSettings);
        try {
            HibernateConnection hibernateConnection = (HibernateConnection) connection();
            if (!hibernateConnection.isConnected()) {
                hibernateConnection.connect();
            }
            sessionManager(new RdbmsSessionManager(hibernateConnection,
                    ((RdbmsStoreSettings) settings).getSessionTimeout().normalized()));
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }


    @Override
    public <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session(false);
        if (entity instanceof BaseEntity) {
            ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
        }
        session.persist(entity);
        return entity;
    }

    @Override
    public <E extends IEntity<?>> E upsertEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws DataStoreException {
        return updateEntity(entity, type, context);
    }

    @Override
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session(false);
        E entity = findEntity(key, type, false, context);
        if (entity != null) {
            session.remove(entity);
            return true;
        }
        return false;
    }

    @Override
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               boolean readOnly,
                                               Context context) throws
            DataStoreException {
        checkState();
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session(readOnly);
        E entity = session.find(type, key);
        if (doRefresh(context)&& entity!=null) {
            session.evict(entity);
        }
        entity = session.find(type, key);
        if (entity instanceof BaseEntity) {
            ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
        }
        return entity;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K extends IKey, E extends IEntity<K>> Cursor<K, E> doSearch(@NonNull Q query,
                                                                        int currentPage,
                                                                        int maxResults,
                                                                        @NonNull Class<? extends K> keyType,
                                                                        @NonNull Class<? extends E> type,
                                                                        boolean readOnly,
                                                                        Context context)
            throws DataStoreException {
        checkState();
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session(readOnly);
        try {

            Query qq;
            if (query.nativeQuery()) {
                qq = session.createNativeQuery(query.generatedQuery(), type);
            } else {
                SqlQueryParser<K, E> parser = (SqlQueryParser<K, E>) getParser(type, keyType);
                parser.parse(query);
                qq = session.createQuery(query.generatedQuery(), type);
            }
            if (query.hasParameters()) {
                for (String key : query.parameters().keySet())
                    qq.setParameter(key, query.parameters().get(key));
            }
            ScrollableResults<E> results = qq.scroll();
            HibernateCursor<K, E> cursor = new HibernateCursor<>(session, results, currentPage);
            return cursor.pageSize(maxResults);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    protected <K extends IKey, E extends IEntity<K>> QueryParser<K, E> createParser(@NonNull Class<? extends E> entityType,
                                                                                    @NonNull Class<? extends K> keyTpe) throws Exception {
        return new SqlQueryParser<>(keyTpe, entityType);
    }

    public Long nextSequence(@NonNull EGeneratedType type, @NonNull String name) throws Exception {
        try {
            if (type == EGeneratedType.DB_SEQUENCE) {
                RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
                Session session = sessionManager.session();
                String sequenceQuery = String.format("SELECT nextval('%s')", name);
                NativeQuery<Long> seq = session.createNativeQuery(sequenceQuery, Long.class);
                return seq.getSingleResult();
            } else {
                return nextSequence(name);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

}
