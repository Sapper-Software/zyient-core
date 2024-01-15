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

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.mapper.Mapping;
import io.zyient.core.mapping.model.mapping.MappedResponse;
import io.zyient.core.mapping.pipeline.settings.ExecutablePipelineSettings;
import io.zyient.core.mapping.pipeline.settings.PipelineSettings;
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
public abstract class ExecutablePipeline<E> extends Pipeline {
    private Class<? extends MappedResponse<E>> responseType;
    private Mapping<E> mapping;
    private RulesExecutor<MappedResponse<E>> postProcessor;
    private MapperFactory mapperFactory;

    public ExecutablePipeline<E> withMapperFactory(@NonNull MapperFactory mapperFactory) {
        this.mapperFactory = mapperFactory;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                             @NonNull DataStoreManager dataStoreManager,
                             @NonNull Class<? extends PipelineSettings> settingsType) throws Exception {
        Preconditions.checkNotNull(mapperFactory);
        super.configure(xmlConfig, dataStoreManager, settingsType);
        ExecutablePipelineSettings settings = (ExecutablePipelineSettings) settings();
        responseType = (Class<? extends MappedResponse<E>>) settings().getResponseType();
        mapping = mapperFactory.getMapping(settings.getMapping());
        if (mapping == null) {
            throw new ConfigurationException(String.format("Specified mapping not found. [mapping=%s]",
                    settings.getMapping()));
        }
        mapping.withTerminateOnValidationError(settings.isTerminateOnValidationError());

        checkAndLoadRules(config());
    }


    @SuppressWarnings("unchecked")
    private void checkAndLoadRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, RuleConfigReader.__CONFIG_PATH)) {
            postProcessor = (RulesExecutor<MappedResponse<E>>) new RulesExecutor<>(responseType)
                    .terminateOnValidationError(settings().isTerminateOnValidationError())
                    .contentDir(contentDir())
                    .configure(xmlConfig);
        }
    }
}
