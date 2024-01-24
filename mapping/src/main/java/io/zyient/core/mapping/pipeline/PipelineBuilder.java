/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.ContentInfo;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.InputReaderFactory;
import io.zyient.core.mapping.readers.MappingContextProvider;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineBuilder {
    public static final String __CONFIG_PATH = "pipelines";
    public static final String __CONFIG_NODE_PIPELINE = "pipeline";

    private final Map<String, Pipeline> transformers = new HashMap<>();
    private MappingContextProvider contextProvider;
    private MapperFactory mapperFactory;
    private InputReaderFactory readerFactory;

    @SuppressWarnings("unchecked")
    public PipelineBuilder configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            mapperFactory = new MapperFactory()
                    .init(xmlConfig, env);
            readerFactory = new InputReaderFactory()
                    .init(xmlConfig);
            HierarchicalConfiguration<ImmutableNode> cConfig =
                    xmlConfig.configurationAt(MappingContextProvider.__CONFIG_PATH);
            Class<? extends MappingContextProvider> cpc =
                    (Class<? extends MappingContextProvider>) ConfigReader.readType(cConfig);
            contextProvider = cpc.getDeclaredConstructor()
                    .newInstance()
                    .configure(cConfig);
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH);
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_NODE_PIPELINE);
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                Class<? extends Pipeline> cls =
                        (Class<? extends Pipeline>) ConfigReader.readType(node);
                Pipeline pipeline = cls.getDeclaredConstructor()
                        .newInstance()
                        .contextProvider(contextProvider)
                        .contentDir(mapperFactory.contentDir())
                        .configure(node, mapperFactory, env);
                transformers.put(pipeline.name(), pipeline);
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public PipelineHandle build(@NonNull ContentInfo contentInfo) throws Exception {
        if (contentInfo instanceof InputContentInfo) {
            return buildInputPipeline((InputContentInfo) contentInfo);
        }
        throw new Exception(String.format("Content type not supported. [type=%s]", contentInfo.getClass().getCanonicalName()));
    }


    @SuppressWarnings("unchecked")
    public PipelineHandle buildInputPipeline(@NonNull InputContentInfo inputContentInfo) throws Exception {
        inputContentInfo = contextProvider.inputContext(inputContentInfo);
        if (inputContentInfo == null) {
            throw new Exception("Failed to parse content info...");
        }
        InputReader reader = readerFactory.getReader(inputContentInfo);
        if (reader == null) {
            DefaultLogger.trace("Failed to get input reader", inputContentInfo);
            throw new Exception("Failed to get input reader...");
        }
        Pipeline pipeline = transformers.get(inputContentInfo.mapping());
        return new PipelineHandle()
                .pipeline(pipeline)
                .reader(reader);
    }
}
