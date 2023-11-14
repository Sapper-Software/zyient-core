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

package io.zyient.base.core.stores.impl.rdbms;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.stores.*;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.stores.impl.settings.rdbms.RdbmsStoreSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import java.util.Collection;
import java.util.List;

public class RdbmsDataStore extends TransactionDataStore<Session, Transaction> {
    public void flush() throws DataStoreException {
        checkState();
        Session session = sessionManager().session();
        if (session != null) {
            session.flush();
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <E extends IEntity<?>> E createEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session();
        if (entity instanceof BaseEntity) {
            if (((BaseEntity) entity).getState().getState() != EEntityState.New) {
                entity = (E) sessionManager.checkCache((BaseEntity<?>) entity);
            }
        }
        IDGenerator.process(entity, this);
        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error saving entity. [type=%s][key=%s]", type.getCanonicalName(), entity.entityKey()));
        }
        if (entity instanceof BaseEntity) {
            entity = (E) sessionManager.updateCache((BaseEntity<?>) entity, EEntityState.Synced);
        }
        return entity;
    }

    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkState(settings instanceof RdbmsStoreSettings);
        try {
            HibernateConnection hibernateConnection = (HibernateConnection) connection();
            sessionManager(new RdbmsSessionManager(hibernateConnection,
                    ((RdbmsStoreSettings) settings).getSessionTimeout().normalized()));
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }


    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session();
        if (entity instanceof BaseEntity) {
            if (((BaseEntity) entity).getState().getState() != EEntityState.New) {
                entity = (E) sessionManager.checkCache((BaseEntity<?>) entity);
            }
        }
        Object result = session.save(entity);
        if (result == null) {
            throw new DataStoreException(String.format("Error updating entity. [type=%s][key=%s]", type.getCanonicalName(), entity.entityKey()));
        }
        if (entity instanceof BaseEntity) {
            entity = (E) sessionManager.updateCache((BaseEntity<?>) entity, EEntityState.Synced);
        }
        return entity;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> type,
                                                       Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session();
        E entity = findEntity(key, type, context);
        if (entity != null) {
            session.delete(entity);
            if (entity instanceof BaseEntity) {
                entity = (E) sessionManager.updateCache((BaseEntity<?>) entity, EEntityState.Deleted);
            }
            return true;
        }
        return false;
    }

    @Override
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> type,
                                               Context context) throws
            DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session();

        E entity = session.find(type, key);
        if (entity instanceof BaseEntity) {
            ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
        }
        return entity;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K extends IKey, E extends IEntity<K>> BaseSearchResult<E> doSearch(@NonNull Q query,
                                                                               int offset,
                                                                               int maxResults,
                                                                               @NonNull Class<? extends K> keyType,
                                                                               @NonNull Class<? extends E> type,
                                                                               Context context)
            throws DataStoreException {
        checkState();
        Preconditions.checkState(isInTransaction());
        RdbmsSessionManager sessionManager = (RdbmsSessionManager) sessionManager();
        Session session = sessionManager.session();
        try {
            SqlQueryParser<K, E> parser = (SqlQueryParser<K, E>) getParser(type, keyType);
            parser.parse(query);
            Query qq = session.createQuery(query.generatedQuery(), type)
                    .setMaxResults(maxResults)
                    .setFirstResult(offset);
            if (query.hasParameters()) {
                for (String key : query.parameters().keySet())
                    qq.setParameter(key, query.parameters().get(key));
            }
            List<E> result = qq.getResultList();
            if (result != null && !result.isEmpty()) {
                for (E entity : result) {
                    if (entity instanceof BaseEntity) {
                        ((BaseEntity) entity).getState().setState(EEntityState.Synced);
                    }
                }
                EntitySearchResult<E> er = new EntitySearchResult<>(type);
                er.setQuery(query.generatedQuery());
                er.setOffset(offset);
                er.setCount(result.size());
                er.setEntities((Collection<E>) result);
                return er;
            }
            return null;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
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

    @Override
    protected <K extends IKey, E extends IEntity<K>> QueryParser<K, E> createParser(@NonNull Class<? extends E> entityType,
                                                                                    @NonNull Class<? extends K> keyTpe) throws Exception {
        return new SqlQueryParser<>(keyTpe, entityType);
    }
}
