/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.pipeline;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.mapping.model.EntityValidationError;
import io.zyient.core.mapping.model.MappedResponse;
import io.zyient.core.mapping.pipeline.settings.PersistencePipelineSettings;
import io.zyient.core.mapping.rules.RuleValidationError;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.TransactionDataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PersistencePipeline<K extends IKey, E extends IEntity<K>> extends Pipeline {
    private Class<? extends MappedResponse<E>> responseType;
    private Class<? extends E> entityType;
    private Class<? extends K> keyType;
    private AbstractDataStore<?> dataStore;

    @Override
    @SuppressWarnings("unchecked")
    protected void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                             @NonNull DataStoreManager dataStoreManager,
                             @NonNull Class<? extends PipelineSettings> settingsType) throws Exception {
        super.configure(xmlConfig, dataStoreManager, settingsType);
        PersistencePipelineSettings settings = (PersistencePipelineSettings) settings();
        responseType = (Class<? extends MappedResponse<E>>) settings.getResponseType();
        entityType = (Class<? extends E>) settings.getEntityType();
        keyType = (Class<? extends K>) settings.getKeyType();
        dataStore = dataStoreManager.getDataStore(settings.getDataStore(), settings.getDataStoreType());
        if (dataStore == null) {
            throw new ConfigurationException(String.format("DataStore not found. [name=%s][type=%s]",
                    settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
        }
    }

    protected E save(@NonNull E entity,
                     Context context) throws Exception {
        entity = dataStore().create(entity, entityType, context);
        if (DefaultLogger.isTraceEnabled()) {
            String json = JSONUtils.asString(entity, entityType);
            DefaultLogger.trace(json);
        }
        return entity;
    }

    @SuppressWarnings("unchecked")
    protected void save(@NonNull E entity,
                        @NonNull ValidationExceptions errors,
                        Context context) throws Exception {
        if (settings().isSaveValidationErrors()) {
            for (ValidationException error : errors) {
                if (error instanceof RuleValidationError) {
                    EntityValidationError ve = new EntityValidationError(entity.entityKey().stringKey(),
                            (Class<? extends IEntity<?>>) entity.getClass(),
                            (RuleValidationError) error);
                    dataStore().upsert(ve, ve.getClass(), context);
                }
            }
        }
    }

    protected void beingTransaction() throws Exception {
        if (dataStore() instanceof TransactionDataStore<?, ?>) {
            ((TransactionDataStore<?, ?>) dataStore()).beingTransaction();
        }
    }

    protected void rollback() throws Exception {
        if (dataStore instanceof TransactionDataStore<?, ?>) {
            if (((TransactionDataStore<?, ?>) dataStore).isInTransaction()) {
                ((TransactionDataStore<?, ?>) dataStore).rollback(true);
            }
        }
    }

    protected void commit() throws Exception {
        if (dataStore instanceof TransactionDataStore<?, ?>) {
            if (((TransactionDataStore<?, ?>) dataStore).isInTransaction()) {
                ((TransactionDataStore<?, ?>) dataStore).commit();
            } else {
                throw new Exception("No active transactions...");
            }
        }
    }
}
