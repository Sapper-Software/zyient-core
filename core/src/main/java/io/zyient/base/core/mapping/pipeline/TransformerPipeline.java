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

package io.zyient.base.core.mapping.pipeline;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.mapping.mapper.MapperFactory;
import io.zyient.base.core.mapping.mapper.Mapping;
import io.zyient.base.core.mapping.model.EntityValidationError;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.MappingContextProvider;
import io.zyient.base.core.mapping.readers.ReadCursor;
import io.zyient.base.core.mapping.readers.ReadResponse;
import io.zyient.base.core.mapping.rules.RuleConfigReader;
import io.zyient.base.core.mapping.rules.RuleValidationError;
import io.zyient.base.core.mapping.rules.RulesExecutor;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.DataStoreManager;
import io.zyient.base.core.stores.TransactionDataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class TransformerPipeline<K extends IKey, E extends IEntity<K>> {
    private Class<? extends E> entityType;
    private Class<? extends K> keyType;
    private AbstractDataStore<?> dataStore;
    private Mapping<E> mapping;
    private RulesExecutor<E> postProcessor;
    private TransformerPipelineSettings settings;
    private MappingContextProvider contextProvider;
    private File contentDir;

    public String name() {
        Preconditions.checkNotNull(mapping);
        return mapping.name();
    }

    @SuppressWarnings("unchecked")
    public TransformerPipeline<K, E> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                               @NonNull MapperFactory mapperFactory,
                                               @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, null, TransformerPipelineSettings.class);
            reader.read();
            settings = (TransformerPipelineSettings) reader.settings();
            mapping = mapperFactory.getMapping(settings.getMapping());
            if (mapping == null) {
                throw new ConfigurationException(String.format("Specified mapping not found. [mapping=%s]",
                        settings.getMapping()));
            }
            entityType = (Class<? extends E>) settings.getEntityType();
            keyType = (Class<? extends K>) settings.getKeyType();
            dataStore = dataStoreManager.getDataStore(settings.getDataStore(), settings().getDataStoreType());
            if (dataStore == null) {
                throw new ConfigurationException(String.format("DataStore not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            checkAndLoadRules(reader.config());
            return this;
        } catch (Exception ex) {
            settings = null;
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }


    @SuppressWarnings("unchecked")
    private void checkAndLoadRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, RuleConfigReader.__CONFIG_PATH)) {
            postProcessor = (RulesExecutor<E>) new RulesExecutor<>(entityType)
                    .contentDir(contentDir)
                    .configure(xmlConfig);
        }
    }

    @SuppressWarnings(("unchecked"))
    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
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
                    if (dataStore instanceof TransactionDataStore<?,?>) {
                        ((TransactionDataStore<?,?>) dataStore).beingTransaction();
                    }
                    committed = false;
                }
                Map<String, Object> data = cursor.next();
                if (data == null) break;
                MappedResponse<E> r = mapping.read(data, context);
                if (postProcessor != null) {
                    postProcessor.evaluate(r, settings().isTerminateOnValidationError());
                }
                E entity = dataStore.create(r.entity(), entityType, context);
                if (DefaultLogger.isTraceEnabled()) {
                    String json = JSONUtils.asString(entity, entityType);
                    DefaultLogger.trace(json);
                }
                if (r.errors() != null) {
                    ValidationExceptions errors = r.errors();
                    if (settings().isSaveValidationErrors()) {
                        for (ValidationException error : errors) {
                            if (error instanceof RuleValidationError) {
                                EntityValidationError ve = new EntityValidationError(entity.entityKey().stringKey(),
                                        (Class<? extends IEntity<?>>) entity.getClass(),
                                        (RuleValidationError) error);
                                dataStore.create(ve, ve.getClass(), context);
                            }
                        }
                    }
                    throw errors;
                }
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

    private void rollback() throws Exception {
        if (dataStore instanceof TransactionDataStore<?,?>) {
            if (((TransactionDataStore<?, ?>) dataStore).isInTransaction()) {
                ((TransactionDataStore<?, ?>) dataStore).rollback();
            }
        }
    }

    private void commit() throws Exception {
        if (dataStore instanceof TransactionDataStore<?,?>) {
            if (((TransactionDataStore<?, ?>) dataStore).isInTransaction()) {
                ((TransactionDataStore<?, ?>) dataStore).commit();
            } else {
                throw new Exception("No active transactions...");
            }
        }
    }
}
