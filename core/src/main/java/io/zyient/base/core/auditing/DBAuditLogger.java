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

package io.zyient.base.core.auditing;

import com.google.common.base.Preconditions;
import io.zyient.base.common.audit.AuditRecord;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.IKeyed;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.BaseSearchResult;
import io.zyient.base.core.stores.impl.EntitySearchResult;
import lombok.NonNull;
import org.hibernate.Session;

import java.io.IOException;
import java.util.*;

public class DBAuditLogger extends AbstractAuditLogger<Session> {
    protected DBAuditLogger() {
        super(AuditLoggerSettings.class);
    }

    /**
     * Search for entity records based on the query string specified.
     *
     * @param query      - Query String
     * @param entityType - Record Entity type.
     * @param serializer - Entity data serializer
     * @return - Collection of fetched records.
     * @throws AuditException
     */
    @Override
    public <T extends IKeyed<?>> Collection<T> search(@NonNull String query,
                                                   @NonNull Class<? extends T> entityType,
                                                   @NonNull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStoreManager() != null);
        try {
            state().check(ProcessorState.EProcessorState.Running);
            AbstractDataStore<Session> dataStore = getDataStore(false);

            String qstr = String.format("FROM %s WHERE id.recordType = :recordType AND (%s)",
                    AuditRecord.class.getCanonicalName(), query);
            Map<String, Object> params = new HashMap<>();
            params.put("recordType", entityType.getCanonicalName());
            BaseSearchResult<AuditRecord> result = dataStore.search(qstr, params, AuditRecord.class, null);
            if (result instanceof EntitySearchResult<AuditRecord>) {
                EntitySearchResult<AuditRecord> er = (EntitySearchResult<AuditRecord>) result;
                Collection<AuditRecord> records = er.getEntities();
                if (records != null && !records.isEmpty()) {
                    List<T> entities = new ArrayList<>(records.size());
                    for (AuditRecord record : records) {
                        T entity = (T) serializer.deserialize(record.getEntityData(), entityType);
                        DefaultLogger.trace(entity);
                        entities.add(entity);
                    }
                    return entities;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    /**
     * Fetch all audit records for the specified entity type and entity key.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @return - List of audit records.
     * @throws AuditException
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K extends IKey, T extends IKeyed<K>> Collection<AuditRecord> find(@NonNull K key,
                                                                              @NonNull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkState(dataStoreManager() != null);
        try {
            state().check(ProcessorState.EProcessorState.Running);
            AbstractDataStore<Session> dataStore = getDataStore(false);

            String qstr = String.format("FROM %s WHERE id.recordType = :recordType AND entityId = :entityId",
                    AuditRecord.class.getCanonicalName());
            Map<String, Object> params = new HashMap<>();
            params.put("recordType", entityType.getCanonicalName());
            params.put("entityId", key.toString());
            BaseSearchResult<AuditRecord> result = dataStore.search(qstr, params, AuditRecord.class, null);
            if (result instanceof EntitySearchResult) {
                EntitySearchResult<AuditRecord> er = (EntitySearchResult<AuditRecord>) result;
                Collection<AuditRecord> records = er.getEntities();
                if (records != null && !records.isEmpty()) {
                    return records;
                }
            }
            return null;
        } catch (Throwable ex) {
            throw new AuditException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (state().getState() == ProcessorState.EProcessorState.Running) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
    }
}
