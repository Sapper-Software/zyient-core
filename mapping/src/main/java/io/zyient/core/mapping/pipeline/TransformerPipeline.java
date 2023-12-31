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

package io.zyient.core.mapping.pipeline;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.mapper.Mapping;
import io.zyient.core.mapping.model.*;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.ReadResponse;
import io.zyient.core.mapping.rules.RuleConfigReader;
import io.zyient.core.mapping.rules.RuleValidationError;
import io.zyient.core.mapping.rules.RulesExecutor;
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
public class TransformerPipeline<K extends IKey, E extends IEntity<K>> extends Pipeline {
    private Class<? extends MappedResponse<E>> responseType;
    private Class<? extends E> entityType;
    private Class<? extends K> keyType;
    private Mapping<E> mapping;
    private RulesExecutor<MappedResponse<E>> postProcessor;
    private AbstractDataStore<?> dataStore;

    @Override
    public String name() {
        Preconditions.checkNotNull(mapping);
        return mapping.name();
    }

    @SuppressWarnings("unchecked")
    public TransformerPipeline<K, E> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                               @NonNull MapperFactory mapperFactory,
                                               @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            super.configure(xmlConfig, dataStoreManager, TransformerPipelineSettings.class);
            TransformerPipelineSettings settings = (TransformerPipelineSettings) settings();
            mapping = mapperFactory.getMapping(settings.getMapping());
            if (mapping == null) {
                throw new ConfigurationException(String.format("Specified mapping not found. [mapping=%s]",
                        settings.getMapping()));
            }
            mapping.withTerminateOnValidationError(settings.isTerminateOnValidationError());
            responseType = (Class<? extends MappedResponse<E>>) settings.getResponseType();
            entityType = (Class<? extends E>) settings.getEntityType();
            keyType = (Class<? extends K>) settings.getKeyType();
            dataStore = dataStoreManager.getDataStore(settings.getDataStore(), settings.getDataStoreType());
            if (dataStore == null) {
                throw new ConfigurationException(String.format("DataStore not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            checkAndLoadRules(config());
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }


    @SuppressWarnings("unchecked")
    private void checkAndLoadRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        TransformerPipelineSettings settings = (TransformerPipelineSettings) settings();
        if (ConfigReader.checkIfNodeExists(xmlConfig, RuleConfigReader.__CONFIG_PATH)) {
            postProcessor = (RulesExecutor<MappedResponse<E>>) new RulesExecutor<>(responseType)
                    .terminateOnValidationError(settings.isTerminateOnValidationError())
                    .contentDir(contentDir())
                    .configure(xmlConfig);
        }
    }

    @SuppressWarnings(("unchecked"))
    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        TransformerPipelineSettings settings = (TransformerPipelineSettings) settings();
        Preconditions.checkNotNull(settings);
        DefaultLogger.info(String.format("Running pipeline for entity. [type=%s]", entityType.getCanonicalName()));
        ReadResponse response = new ReadResponse();
        ReadCursor cursor = reader.open();
        int count = 0;
        int errorCount = 0;
        boolean committed = true;
        int commitCount = 0;
        while (true) {
            try {
                if (committed) {
                    if (dataStore() instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore()).beingTransaction();
                    }
                    committed = false;
                }
                SourceMap data = cursor.next();
                if (data == null) break;
                process(data, context);
                count++;
                if (commitCount >= settings.getCommitBatchSize()) {
                    commit();
                    committed = true;
                    commitCount = 0;
                }
                commitCount++;
            } catch (ValidationException ex) {
                String mesg = String.format("[file=%s][record=%d] Validation Failed: %s",
                        reader.input().getAbsolutePath(), count, ex.getLocalizedMessage());
                ValidationExceptions ve = ValidationExceptions.add(new ValidationException(mesg), null);
                if (settings().isTerminateOnValidationError()) {
                    DefaultLogger.stacktrace(ex);
                    rollback();
                    throw ve;
                } else {
                    errorCount++;
                    commitCount++;
                    DefaultLogger.warn(mesg);
                    response.add(ve);
                }
            } catch (ValidationExceptions vex) {
                if (settings().isTerminateOnValidationError()) {
                    rollback();
                    throw vex;
                } else {
                    errorCount++;
                    commitCount++;
                    DefaultLogger.stacktrace(vex);
                    response.add(vex);
                }
            } catch (Exception e) {
                rollback();
                DefaultLogger.stacktrace(e);
                DefaultLogger.error(e.getLocalizedMessage());
                throw e;
            }
        }
        if (!committed && commitCount > 0) {
            commit();
        }
        DefaultLogger.info(String.format("Processed [%d] records for entity. [type=%s]",
                count, entityType.getCanonicalName()));
        return response.recordCount(count)
                .errorCount(errorCount);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(@NonNull SourceMap data, Context context) throws Exception {
        EvaluationStatus ret = null;
        MappedResponse<E> r = mapping.read(data, context);
        ret = r.getStatus();
        if (ret.status() == StatusCode.Success && postProcessor != null) {
            ret = postProcessor.evaluate(r);
        }
        if (ret.status() != StatusCode.IgnoreRecord) {
            E entity = dataStore().create(r.getEntity(), entityType, context);
            if (DefaultLogger.isTraceEnabled()) {
                String json = JSONUtils.asString(entity, entityType);
                DefaultLogger.trace(json);
            }
            if (ret.errors() != null) {
                ValidationExceptions errors = ret.errors();
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
                throw errors;
            }
        } else if (DefaultLogger.isTraceEnabled()) {
            DefaultLogger.trace("RECORD IGNORED", data);
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
