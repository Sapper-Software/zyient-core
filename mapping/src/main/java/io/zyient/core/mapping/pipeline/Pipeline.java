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
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.model.StatusCode;
import io.zyient.core.mapping.readers.MappingContextProvider;
import io.zyient.core.persistence.DataStoreManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class Pipeline {
    private PipelineSettings settings;
    private MappingContextProvider contextProvider;
    private HierarchicalConfiguration<ImmutableNode> config;
    private File contentDir;

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    protected void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                             @NonNull DataStoreManager dataStoreManager,
                             @NonNull Class<? extends PipelineSettings> settingsType) throws Exception {
        ConfigReader reader = new ConfigReader(xmlConfig, null, settingsType);
        reader.read();
        settings = (PipelineSettings) reader.settings();

        config = reader.config();
    }

    protected RecordResponse errorResponse(RecordResponse response,
                                           SourceMap source,
                                           @NonNull Exception error) {
        if (response == null) {
            response = new RecordResponse();
        }
        if (source != null) {
            response.setSource(source);
        }
        ValidationExceptions ex = null;
        if (error instanceof ValidationExceptions) {
            ex = (ValidationExceptions) error;
        } else if (error instanceof ValidationException) {
            ex = ValidationExceptions.add((ValidationException) error, ex);
        } else {
            ex = ValidationExceptions.add(new ValidationException(error), ex);
        }
        EvaluationStatus status = new EvaluationStatus();
        status.setStatus(StatusCode.ValidationFailed);
        status.setErrors(ex);
        response.setStatus(status);
        return response;
    }

    public abstract Pipeline configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                       @NonNull MapperFactory mapperFactory,
                                       @NonNull DataStoreManager dataStoreManager) throws ConfigurationException;

    public abstract RecordResponse process(@NonNull SourceMap data, Context context) throws Exception;

}
