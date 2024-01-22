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

package io.zyient.core.persistence.impl.rdbms;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.StoreSessionManager;
import io.zyient.core.persistence.model.BaseEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.HashMap;
import java.util.Map;

public class RdbmsSessionManager extends StoreSessionManager<Session, Transaction> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class TransactionCacheElement {
        private Transaction tx;
        private String key;
        private BaseEntity<?> entity;

        public static String generateKey(BaseEntity<?> entity) {
            return String.format("%s[%s]", entity.getClass(), entity.entityKey().stringKey());
        }
    }

    private final HibernateConnection hibernateConnection;
    private final Map<Long, Map<String, TransactionCacheElement>> transactionCache = new HashMap<>();

    public RdbmsSessionManager(@NonNull HibernateConnection hibernateConnection,
                               long sessionTimeout) {
        super(sessionTimeout);
        this.hibernateConnection = hibernateConnection;
    }

    @Override
    protected boolean isActive(@NonNull Transaction transaction) {
        return transaction.isActive();
    }

    @Override
    protected boolean isAvailable(@NonNull Session session) {
        return session.isOpen();
    }

    @Override
    protected Session create() throws DataStoreException {
        try {
            Session session = hibernateConnection.getConnection();
            session.setCacheMode(CacheMode.IGNORE);
            return session;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    protected void close(@NonNull Session session) throws DataStoreException {
        if (session.isOpen()) {
            session.close();
        }
        if (!remove()) {
            long tid = Thread.currentThread().getId();
            DefaultLogger.warn(String.format("[THREAD=%d] Failed to remove session...", tid));
        }
        clearCache();
    }

    @Override
    protected Transaction beingTransaction(@NonNull Session session) throws DataStoreException {
        Preconditions.checkArgument(session.isOpen());
        if (session.isJoinedToTransaction()) {
            return session.getTransaction();
        }
        return session.beginTransaction();
    }

    @Override
    protected void commit(@NonNull Session session,
                          @NonNull Transaction transaction) throws DataStoreException {
        Preconditions.checkArgument(session.isOpen());
        if (!session.isJoinedToTransaction()) {
            throw new DataStoreException("Session has no active transaction...");
        }
        if (!transaction.isActive()) {
            throw new DataStoreException("Transaction instance is not active...");
        }
        session.flush();
        transaction.commit();
    }

    @Override
    protected void rollback(@NonNull Session session, @NonNull Transaction transaction) throws DataStoreException {
        Preconditions.checkArgument(session.isOpen());
        if (!session.isJoinedToTransaction()) {
            throw new DataStoreException("Session has no active transaction...");
        }
        if (!transaction.isActive()) {
            throw new DataStoreException("Transaction instance is not active...");
        }
        transaction.rollback();
    }

    private void clearCache() throws DataStoreException {
        long tid = Thread.currentThread().getId();
        Map<String, TransactionCacheElement> cache = transactionCache.remove(tid);
        if (cache != null) {
            cache.clear();
        }
    }

    public BaseEntity<?> checkCache(@NonNull BaseEntity<?> entity) throws DataStoreException {
        Preconditions.checkState(isInTransaction());
        if (entity.getState().getState() == EEntityState.New)
            return entity;
        Map<String, TransactionCacheElement> cache;
        synchronized (transactionCache) {
            long tid = Thread.currentThread().getId();
            cache = transactionCache.computeIfAbsent(tid, k -> new HashMap<>());
        }
        String key = TransactionCacheElement.generateKey((BaseEntity<?>) entity);
        TransactionCacheElement ce = cache.get(key);
        if (ce != null) {
            Transaction tx = transaction();
            if (!tx.equals(ce.tx)) {
                throw new DataStoreException(
                        String.format("Invalid transaction cache: transaction handle is stale. [entity=%s]",
                                entity.entityKey().stringKey()));
            }
            if (entity.getState().getState() != EEntityState.Synced) {
                throw new DataStoreException(
                        String.format("Invalid entity state. [entity=%s][state=%s]",
                                entity.entityKey().stringKey(), entity.getState().getState().name()));
            }
            return entity;
        } else
            throw new DataStoreException(
                    String.format("Invalid entity state: [state=%s][id=%s]",
                            entity.getState().getState().name(), entity.entityKey().stringKey()));
    }

    public BaseEntity<?> updateCache(@NonNull BaseEntity<?> entity, @NonNull EEntityState state) throws DataStoreException {
        Preconditions.checkState(isInTransaction());
        Map<String, TransactionCacheElement> cache;
        synchronized (transactionCache) {
            long tid = Thread.currentThread().getId();
            cache = transactionCache.computeIfAbsent(tid, k -> new HashMap<>());
        }
        Transaction tx = transaction();
        entity.getState().setState(state);
        TransactionCacheElement ce = new TransactionCacheElement();
        ce.entity = (BaseEntity<?>) entity;
        ce.key = TransactionCacheElement.generateKey(entity);
        ce.tx = tx;

        cache.put(ce.key, ce);

        return entity;
    }
}
