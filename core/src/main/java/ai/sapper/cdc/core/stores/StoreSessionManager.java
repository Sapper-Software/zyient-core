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

package ai.sapper.cdc.core.stores;

import ai.sapper.cdc.core.stores.DataStoreException;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class StoreSessionManager<C, T> {
    @Getter
    @Accessors(fluent = true)
    public static class StoreSession<C, T> {
        private C session;
        private T transaction;
        private long timeLastUsed = System.currentTimeMillis();

        public boolean hasTransaction() {
            return (transaction != null);
        }
    }

    private final Map<Long, StoreSession<C, T>> sessions = new HashMap<>();
    private final long sessionTimeout;

    public StoreSessionManager(long sessionTimeout) {
        Preconditions.checkArgument(sessionTimeout > 0);
        this.sessionTimeout = sessionTimeout;
    }

    public C session() throws DataStoreException {
        synchronized (sessions) {
            long tid = Thread.currentThread().getId();
            if (sessions.containsKey(tid)) {
                StoreSession<C, T> session = sessions.get(tid);
                if (session.session != null) {
                    long delta = System.currentTimeMillis() - session.timeLastUsed;
                    if (delta < sessionTimeout) {
                        session.timeLastUsed = System.currentTimeMillis();
                        return session.session();
                    } else {
                        close(session.session);
                    }
                }
            }
            C session = create();
            StoreSession<C, T> ss = new StoreSession<>();
            ss.session = session;
            sessions.put(tid, ss);
            return session;
        }
    }

    public void beingTransaction() throws DataStoreException {
        long tid = Thread.currentThread().getId();
        if (sessions.containsKey(tid)) {
            StoreSession<C, T> session = sessions.get(tid);
            if (session.hasTransaction()) {
                if (!isActive(session.transaction)) {
                    throw new DataStoreException(String.format("[%d] Invalid transaction handle in cache.", tid));
                }
            } else {
                session.transaction = beingTransaction(session.session);
            }
            session.timeLastUsed = System.currentTimeMillis();
        } else {
            throw new DataStoreException(String.format("[%d] No active sessions found.", tid));
        }
    }

    public void commit() throws DataStoreException {
        long tid = Thread.currentThread().getId();
        if (sessions.containsKey(tid)) {
            StoreSession<C, T> session = sessions.get(tid);
            if (session.hasTransaction()) {
                if (!isActive(session.transaction)) {
                    throw new DataStoreException(String.format("[%d] Invalid transaction handle in cache.", tid));
                }
            }
            commit(session.session, session.transaction);
            session.transaction = null;
            session.timeLastUsed = System.currentTimeMillis();
        } else {
            throw new DataStoreException(String.format("[%d] No active sessions found.", tid));
        }
    }

    public void rollback() throws DataStoreException {
        long tid = Thread.currentThread().getId();
        if (sessions.containsKey(tid)) {
            StoreSession<C, T> session = sessions.get(tid);
            if (session.hasTransaction()) {
                if (!isActive(session.transaction)) {
                    throw new DataStoreException(String.format("[%d] Invalid transaction handle in cache.", tid));
                }
            }
            rollback(session.session, session.transaction);
            session.transaction = null;
            session.timeLastUsed = System.currentTimeMillis();
        } else {
            throw new DataStoreException(String.format("[%d] No active sessions found.", tid));
        }
    }

    public void close() throws DataStoreException {
        synchronized (sessions) {
            long tid = Thread.currentThread().getId();
            if (sessions.containsKey(tid)) {
                StoreSession<C, T> session = sessions.remove(tid);
                close(session.session);
            }
        }
    }

    public T transaction() {
        long tid = Thread.currentThread().getId();
        if (sessions.containsKey(tid)) {
            StoreSession<C, T> session = sessions.get(tid);
            if (session.hasTransaction()) {
                return session.transaction;
            }
        }
        return null;
    }

    public boolean isInTransaction() {
        long tid = Thread.currentThread().getId();
        if (sessions.containsKey(tid)) {
            StoreSession<C, T> session = sessions.get(tid);
            if (session.hasTransaction()) {
                return isActive(session.transaction);
            }
        }
        return false;
    }

    protected abstract boolean isActive(@NonNull T transaction);

    protected abstract C create() throws DataStoreException;

    protected abstract void close(@NonNull C session) throws DataStoreException;

    protected abstract T beingTransaction(@NonNull C session) throws DataStoreException;

    protected abstract void commit(@NonNull C session, @NonNull T transaction) throws DataStoreException;

    protected abstract void rollback(@NonNull C session, @NonNull T transaction) throws DataStoreException;
}
