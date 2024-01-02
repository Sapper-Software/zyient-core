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
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.pipeline.settings.CompositePipelineSettings;
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
public abstract class CompositePipeline extends Pipeline {
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
            settings.read(config());
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
                PipelineInfo pi = settings.getPipelineContext().get(key);
                if (!pipelines.containsKey(pi.getPipeline())) {
                    throw new Exception(String.format("Pipeline definition missing. [path=%s][pipeline=%s]",
                            key, pi.getPipeline()));
                }
            }
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public RecordResponse execute(@NonNull SourceMap data, Context context) throws Exception {
        CompositePipelineSettings settings = (CompositePipelineSettings) settings();
        Preconditions.checkNotNull(settings);
        Context ctx = reset(context);
        RecordResponse response = null;
        for (String path : settings.getPipelineContext().keySet()) {
            PipelineInfo pi = settings.getPipelineContext().get(path);
            if (pi.isResetContext()) {
                ctx = reset(context);
            }
            Object value = evaluate(data, path);
            if (value != null) {
                Pipeline pipeline = pipelines.get(path);
                Preconditions.checkNotNull(pipeline);
                if (value instanceof List<?>) {
                    List<Object> values = (List<Object>) value;
                    for (Object v : values) {
                        SourceMap d = new SourceMap((Map<String, ?>) v);
                        response = pipeline.process(d, ctx);
                        checkAndAddContext(response, pi, ctx);
                    }
                } else {
                    SourceMap d = new SourceMap((Map<String, ?>) value);
                    response = pipeline.process(data, ctx);
                    checkAndAddContext(response, pi, ctx);
                }
            } else {
                String mesg = String.format("No value found for path. [path=%s]", path);
                if (!pi.isIgnorable()) {
                    throw new Exception(mesg);
                }
                DefaultLogger.warn(mesg);
            }
        }
        return response;
    }

    private void checkAndAddContext(RecordResponse response,
                                    PipelineInfo pi,
                                    Context context) {
        Object ret = response.getEntity();
        if (ret != null && pi.isAddToContext()) {
            String key = ret.getClass().getSimpleName();
            if (!Strings.isNullOrEmpty(pi.getContextKey())) {
                key = pi.getContextKey();
            }
            context.put(key, ret);
        }
    }

    protected abstract Object evaluate(SourceMap data, String expression) throws Exception;

    private Context reset(Context context) throws Exception {
        Context ctx = context.getClass()
                .getDeclaredConstructor()
                .newInstance();
        ctx.setParams(context.getParams());
        return ctx;
    }
}
