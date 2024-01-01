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
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.mapper.Mapping;
import io.zyient.core.mapping.model.*;
import io.zyient.core.mapping.pipeline.settings.TransformerPipelineSettings;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.ReadResponse;
import io.zyient.core.mapping.rules.RuleConfigReader;
import io.zyient.core.mapping.rules.RulesExecutor;
import io.zyient.core.persistence.DataStoreManager;
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
public class TransformerPipeline<K extends IKey, E extends IEntity<K>> extends PersistencePipeline<K, E> {
    private Mapping<E> mapping;
    private RulesExecutor<MappedResponse<E>> postProcessor;

    @Override
    public String name() {
        Preconditions.checkNotNull(mapping);
        return mapping.name();
    }

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
            postProcessor = (RulesExecutor<MappedResponse<E>>) new RulesExecutor<>(responseType())
                    .terminateOnValidationError(settings.isTerminateOnValidationError())
                    .contentDir(contentDir())
                    .configure(xmlConfig);
        }
    }

    @SuppressWarnings(("unchecked"))
    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        TransformerPipelineSettings settings = (TransformerPipelineSettings) settings();
        Preconditions.checkNotNull(settings);
        DefaultLogger.info(String.format("Running pipeline for entity. [type=%s]", entityType().getCanonicalName()));
        ReadResponse response = new ReadResponse();
        ReadCursor cursor = reader.open();
        while (true) {
            RecordResponse r = new RecordResponse();
            try {
                SourceMap data = cursor.next();
                if (data == null) break;
                r.setSource(data);
                response.incrementCount();
                r = process(data, context);
                response.add(r);
                response.incrementCommitCount();
            } catch (ValidationException | ValidationExceptions ex) {
                String mesg = String.format("[file=%s][record=%d] Validation Failed: %s",
                        reader.input().getAbsolutePath(), response.getRecordCount(), ex.getLocalizedMessage());
                ValidationExceptions ve = ValidationExceptions.add(new ValidationException(mesg), null);
                if (settings().isTerminateOnValidationError()) {
                    DefaultLogger.stacktrace(ex);
                    throw ve;
                } else {
                    response.incrementCount();
                    DefaultLogger.warn(mesg);
                    r =  errorResponse(r, null, ex);
                    response.add(r);
                }
            } catch (Exception e) {
                rollback();
                DefaultLogger.stacktrace(e);
                DefaultLogger.error(e.getLocalizedMessage());
                throw e;
            }
        }
        DefaultLogger.info(String.format("Processed [%d] records for entity. [type=%s]",
                response.getRecordCount(), entityType().getCanonicalName()));
        return response;
    }

    @Override
    public RecordResponse process(@NonNull SourceMap data, Context context) throws Exception {
        RecordResponse response = new RecordResponse();
        response.setSource(data);
        MappedResponse<E> r = mapping.read(data, context);
        response.setStatus(r.getStatus());
        response.setEntity(r.getEntity());
        if (response.getStatus().getStatus() == StatusCode.Success && postProcessor != null) {
            EvaluationStatus ret = postProcessor.evaluate(r);
            response.setStatus(ret);
        }
        if (response.getStatus().getStatus() != StatusCode.IgnoreRecord) {
            beingTransaction();
            try {
                E entity = save(r.getEntity(), context);
                ValidationExceptions errors = response.getStatus().getErrors();
                if (errors != null) {
                    if (settings().isTerminateOnValidationError()) {
                        throw errors;
                    }
                    save(entity, errors, context);
                }
                commit();
            } catch (ValidationException | ValidationExceptions ve) {
                rollback();
                throw ve;
            } catch (Throwable t) {
                rollback();
                throw new Exception(t);
            }
        } else if (DefaultLogger.isTraceEnabled()) {
            DefaultLogger.trace("RECORD IGNORED", data);
        }
        return response;
    }
}
