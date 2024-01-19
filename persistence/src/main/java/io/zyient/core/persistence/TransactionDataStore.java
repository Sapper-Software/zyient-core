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

package io.zyient.core.persistence;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class TransactionDataStore<C, T> extends AbstractDataStore<C> {
    private StoreSessionManager<C, T> sessionManager;


    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    protected void checkState() throws DataStoreException {
        super.checkState();
        Preconditions.checkNotNull(sessionManager);
    }


    public boolean isInTransaction() throws DataStoreException {
        checkState();
        return sessionManager().isInTransaction();
    }

    public void beingTransaction() throws DataStoreException {
        checkState();
        C session = sessionManager().session();
        if (session == null) {
            throw new DataStoreException("Failed to create session...");
        }
        sessionManager().beingTransaction();
    }


    public void commit() throws DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        sessionManager.commit();
    }

    public void rollback(boolean raiseError) throws DataStoreException {
        checkState();
        if (!raiseError) {
            if (!isInTransaction()) {
                return;
            }
        }
        sessionManager().rollback();
    }

    public void endSession() throws DataStoreException {
        sessionManager.endSession();
    }

    public void closeSession() throws DataStoreException {
        sessionManager.close();
    }
}
