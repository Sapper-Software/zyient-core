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

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.mapping.mapper.MapperFactory;
import io.zyient.base.core.mapping.mapper.Mapping;
import io.zyient.base.core.mapping.model.ContentInfo;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.ReadCursor;
import io.zyient.base.core.mapping.rules.Rule;
import io.zyient.base.core.mapping.rules.RuleConditionFailed;
import io.zyient.base.core.mapping.rules.RulesExecutor;
import io.zyient.base.core.stores.AbstractDataStore;
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
    private Mapping<E> mapping;
    private AbstractDataStore<?> dataStore;
    private RulesExecutor<E> postProcessor;
    private TransformerPipelineSettings settings;

    public String name() {
        return settings.getName();
    }

    public TransformerPipeline<K, E> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                               @NonNull MapperFactory mapperFactory,
                                               @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, null, TransformerPipelineSettings.class);
            reader.read();
            settings = (TransformerPipelineSettings) reader.settings();
            mapping = mapperFactory.getMapping(settings.getMapper());
            if (mapping == null) {
                throw new Exception(String.format("Specified mapping not found. [mapping=%s]",
                        settings.getMapper()));
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
        if (ConfigReader.checkIfNodeExists(xmlConfig, Rule.__CONFIG_PATH)) {
            postProcessor = (RulesExecutor<E>) new RulesExecutor<>(entityType)
                    .configure(xmlConfig);
        }
    }

    public void run(@NonNull InputReader reader, ContentInfo context) throws Exception {
        if (!Strings.isNullOrEmpty(context.mapping())) {
            if (mapping.name().compareTo(context.mapping()) != 0) {
                throw new Exception(String.format("Mapper mis-match: [expected=%s][specified=%s]",
                        mapping.name(), context.mapping()));
            }
        }
        DefaultLogger.info(String.format("Running pipeline for entity. [type=%s]", entityType.getCanonicalName()));
        ReadCursor cursor = reader.open();
        int count = 0;
        while (true) {
            try {
                Map<String, Object> data = cursor.next();
                if (data == null) break;
                MappedResponse<E> response = mapping.read(data, context);
                if (postProcessor != null) {
                    postProcessor.evaluate(response);
                }
                E entity = dataStore.create(response.entity(), entityType, context);
                if (DefaultLogger.isTraceEnabled()) {
                    String json = JSONUtils.asString(entity, entityType);
                    DefaultLogger.trace(json);
                }
                count++;
            } catch (RuleConditionFailed rf) {
                // Do nothing...
            } catch (ValidationException ex) {
                String mesg = String.format("[file=%s][record=%d] Validation Failed: %s",
                        reader.file().getAbsolutePath(), count, ex.getLocalizedMessage());
                if (settings().isTerminateOnValidationError()) {
                    DefaultLogger.stacktrace(ex);
                    throw new ValidationException(mesg);
                } else {
                    DefaultLogger.warn(mesg);
                }
            } catch (Exception e) {
                DefaultLogger.stacktrace(e);
                DefaultLogger.error(e.getLocalizedMessage());
                throw e;
            }
        }
        DefaultLogger.info(String.format("Processed [%d] records for entity. [type=%s]",
                count, entityType.getCanonicalName()));
    }
}
