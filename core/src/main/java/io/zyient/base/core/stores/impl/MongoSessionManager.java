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

package io.zyient.base.core.stores.impl;

import com.mongodb.client.ClientSession;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.StoreSessionManager;
import lombok.NonNull;

public class MongoSessionManager extends StoreSessionManager<ClientSession, MongoTransaction> {
    private final MongoDSConnection connection;

    public MongoSessionManager(@NonNull MongoDSConnection connection,
                               long sessionTimeout) {
        super(sessionTimeout);
        this.connection = connection;
    }

    @Override
    protected boolean isActive(@NonNull MongoTransaction transaction) {
        return transaction.isActive();
    }

    @Override
    protected ClientSession create() throws DataStoreException {
        try {
            return connection.getConnection().startSession();
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    protected void close(@NonNull ClientSession session) throws DataStoreException {
        session.close();
    }

    @Override
    protected MongoTransaction beingTransaction(@NonNull ClientSession session) throws DataStoreException {
        if (!session.hasActiveTransaction()) {
            session.startTransaction();
        }
        MongoTransaction transaction = new MongoTransaction();
        transaction.setSession(session);
        transaction.setActive(true);
        return transaction;
    }

    @Override
    protected void commit(@NonNull ClientSession session,
                          @NonNull MongoTransaction transaction) throws DataStoreException {
        if (!transaction.isActive() || !transaction.getSession().hasActiveTransaction()) {
            throw new DataStoreException("Transaction instance is not active...");
        }
        transaction.getSession().commitTransaction();
        transaction.setActive(false);
    }

    @Override
    protected void rollback(@NonNull ClientSession session,
                            @NonNull MongoTransaction transaction) throws DataStoreException {
        if (!transaction.isActive() || !transaction.getSession().hasActiveTransaction()) {
            throw new DataStoreException("Transaction instance is not active...");
        }
        transaction.getSession().abortTransaction();
        transaction.setActive(false);
    }
}
