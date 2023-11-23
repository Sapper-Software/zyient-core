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
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.mapping.mapper.MapperFactory;
import io.zyient.base.core.mapping.mapper.Mapping;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.model.OutputContentInfo;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.MappingContextProvider;
import io.zyient.base.core.mapping.readers.ReadCursor;
import io.zyient.base.core.mapping.readers.ReadResponse;
import io.zyient.base.core.mapping.rules.RuleConfigReader;
import io.zyient.base.core.mapping.rules.RulesExecutor;
import io.zyient.base.core.mapping.writers.OutputWriter;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class TransformerPipeline<K extends IKey, E extends IEntity<K>> {
    private Class<? extends E> entityType;
    private Class<? extends K> keyType;
    private Mapping<E> mapping;
    private AbstractDataStore<?> dataStore;
    private RulesExecutor<E> postProcessor;
    private TransformerPipelineSettings settings;
    private MappingContextProvider contextProvider;

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
            entityType = (Class<? extends E>) settings.getEntityType();
            keyType = (Class<? extends K>) settings.getKeyType();
            mapping = mapperFactory.getMapping(settings.getMapper());
            if (mapping == null) {
                throw new Exception(String.format("Specified mapping not found. [mapping=%s]",
                        settings.getMapper()));
            }
            if (contextProvider != null) {
                mapping.withContextProvider(contextProvider);
            }
            dataStore = dataStoreManager.getDataStore(settings.getDataStore(), settings().getDataStoreType());
            if (dataStore == null) {
                throw new ConfigurationException(String.format("DataStore not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            checkAndLoadRules(reader.config());
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }


    @SuppressWarnings("unchecked")
    private void checkAndLoadRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, RuleConfigReader.__CONFIG_PATH)) {
            postProcessor = (RulesExecutor<E>) new RulesExecutor<>(entityType)
                    .configure(xmlConfig);
        }
    }

    public void write(@NonNull OutputWriter writer, @NonNull OutputContentInfo context) throws Exception {
        if (!Strings.isNullOrEmpty(context.mapping())) {
            if (mapping.name().compareTo(context.mapping()) != 0) {
                throw new Exception(String.format("Mapper mis-match: [expected=%s][specified=%s]",
                        mapping.name(), context.mapping()));
            }
        }
        AbstractDataStore.Q query = context.query();
        if (query == null) {
            throw new Exception("No query specified...");
        }
        DefaultLogger.info(String.format("Running pipeline for entity. [type=%s]", entityType.getCanonicalName()));
        Cursor<K, E> result = dataStore.search(query, keyType, entityType, context);
        // TODO: Finish writing
    }

    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        if (!Strings.isNullOrEmpty(context.mapping())) {
            if (mapping.name().compareTo(context.mapping()) != 0) {
                throw new Exception(String.format("Mapper mis-match: [expected=%s][specified=%s]",
                        mapping.name(), context.mapping()));
            }
        }
        DefaultLogger.info(String.format("Running pipeline for entity. [type=%s]", entityType.getCanonicalName()));
        ReadResponse response = new ReadResponse();
        ReadCursor cursor = reader.open();
        int count = 0;
        int errorCount = 0;
        while (true) {
            try {
                Map<String, Object> data = cursor.next();
                if (data == null) break;
                MappedResponse<E> r = mapping.read(data, context);
                if (postProcessor != null) {
                    postProcessor.evaluate(r);
                }
                E entity = dataStore.create(r.entity(), entityType, context);
                if (DefaultLogger.isTraceEnabled()) {
                    String json = JSONUtils.asString(entity, entityType);
                    DefaultLogger.trace(json);
                }
                count++;
            } catch (ValidationException ex) {
                String mesg = String.format("[file=%s][record=%d] Validation Failed: %s",
                        reader.input().getAbsolutePath(), count, ex.getLocalizedMessage());
                ValidationExceptions ve = ValidationExceptions.add(new ValidationException(mesg), null);
                if (settings().isTerminateOnValidationError()) {
                    DefaultLogger.stacktrace(ex);
                    throw ve;
                } else {
                    errorCount++;
                    DefaultLogger.warn(mesg);
                    response.add(ve);
                }
            } catch (ValidationExceptions vex) {
                if (settings().isTerminateOnValidationError()) {
                    throw vex;
                } else {
                    errorCount++;
                    DefaultLogger.stacktrace(vex);
                    response.add(vex);
                }
            } catch (Exception e) {
                DefaultLogger.stacktrace(e);
                DefaultLogger.error(e.getLocalizedMessage());
                throw e;
            }
        }
        DefaultLogger.info(String.format("Processed [%d] records for entity. [type=%s]",
                count, entityType.getCanonicalName()));
        return response.recordCount(count)
                .errorCount(errorCount);
    }
}
