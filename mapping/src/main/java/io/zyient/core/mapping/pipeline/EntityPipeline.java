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
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.StatusCode;
import io.zyient.core.mapping.model.mapping.MappedResponse;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.pipeline.settings.PersistedEntityPipelineSettings;
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
public class EntityPipeline<K extends IKey, E extends IEntity<K>> extends PersistencePipeline<K, E> {

    @Override
    public String name() {
        Preconditions.checkNotNull(mapping());
        return mapping().name();
    }

    public EntityPipeline<K, E> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                          @NonNull MapperFactory mapperFactory,
                                          @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            withMapperFactory(mapperFactory);
            super.configure(xmlConfig, env, PersistedEntityPipelineSettings.class);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public RecordResponse execute(@NonNull SourceMap data, Context context) throws Exception {
        RecordResponse response = new RecordResponse();
        response.setSource(data);
        MappedResponse<E> r = mapping().read(data, context);
        response.setStatus(r.getStatus());
        response.setEntity(r.getEntity());
        if (response.getStatus().getStatus() == StatusCode.Success && postProcessor() != null) {
            EvaluationStatus ret = postProcessor().evaluate(r);
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
