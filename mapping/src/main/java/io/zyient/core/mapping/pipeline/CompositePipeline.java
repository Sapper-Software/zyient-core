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

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadResponse;
import io.zyient.core.persistence.DataStoreManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class CompositePipeline extends Pipeline {
    public static final String __CONFIG_PATH_PIPELINES = "pipelines";
    private Map<String, Pipeline> pipelines;

    @Override
    @SuppressWarnings("unchecked")
    public Pipeline configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull MapperFactory mapperFactory,
                              @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        try {
            configure(xmlConfig, dataStoreManager, CompositePipelineSettings.class);
            CompositePipelineSettings settings = (CompositePipelineSettings) settings();
            HierarchicalConfiguration<ImmutableNode> psConfig = config().configurationAt(__CONFIG_PATH_PIPELINES);
            List<HierarchicalConfiguration<ImmutableNode>> nodes
                    = psConfig.configurationsAt(PipelineBuilder.__CONFIG_NODE_PIPELINE);
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                Class<? extends Pipeline> cls =
                        (Class<? extends Pipeline>) ConfigReader.readType(node);
                Pipeline pipeline = cls.getDeclaredConstructor()
                        .newInstance()
                        .contextProvider(contextProvider())
                        .contentDir(mapperFactory.contentDir())
                        .configure(node, mapperFactory, dataStoreManager);
                pipelines.put(pipeline.name(), pipeline);
            }
            for (String key : settings.getPipelineContext().keySet()) {
                String name = settings.getPipelineContext().get(key);
                if (!pipelines.containsKey(name)) {
                    throw new Exception(String.format("Pipeline definition missing. [path=%s][pipeline=%s]",
                            key, name));
                }
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        return null;
    }

    @Override
    public void process(@NonNull SourceMap data, Context context) throws Exception {

    }
}
