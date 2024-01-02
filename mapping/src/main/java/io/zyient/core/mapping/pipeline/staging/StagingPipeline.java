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

package io.zyient.core.mapping.pipeline.staging;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.*;
import io.zyient.core.mapping.pipeline.ExecutablePipeline;
import io.zyient.core.mapping.pipeline.Pipeline;
import io.zyient.core.mapping.pipeline.settings.ExecutablePipelineSettings;
import io.zyient.core.persistence.DataStoreManager;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class StagingPipeline<E> extends ExecutablePipeline<E> {

    @Override
    public Pipeline configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull MapperFactory mapperFactory,
                              @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            withMapperFactory(mapperFactory);
            super.configure(xmlConfig, dataStoreManager, ExecutablePipelineSettings.class);
            return this;
        } catch (Exception ex) {
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
            try {
                E entity = r.getEntity();
                ValidationExceptions errors = response.getStatus().getErrors();
                if (errors != null) {
                    if (settings().isTerminateOnValidationError()) {
                        throw errors;
                    }
                }
            } catch (ValidationExceptions ve) {
                throw ve;
            } catch (Throwable t) {
                throw new Exception(t);
            }
        } else if (DefaultLogger.isTraceEnabled()) {
            DefaultLogger.trace("RECORD IGNORED", data);
        }
        return response;
    }
}
