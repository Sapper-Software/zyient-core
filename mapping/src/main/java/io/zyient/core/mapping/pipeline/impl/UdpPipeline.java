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

package io.zyient.core.mapping.pipeline.impl;

import com.google.common.base.Strings;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.pipeline.CompositePipeline;
import io.zyient.core.mapping.pipeline.Pipeline;
import io.zyient.core.mapping.pipeline.PipelineInfo;
import io.zyient.core.mapping.pipeline.settings.CompositePipelineSettings;
import io.zyient.core.persistence.DataStoreManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class UdpPipeline extends CompositePipeline {
    public static final String __CONFIG_PATH_FILTERS = "filters";

    private final Map<String, PathFilter> filters = new HashMap<>();

    @Override
    public Pipeline configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull MapperFactory mapperFactory,
                              @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        super.configure(xmlConfig, mapperFactory, dataStoreManager);
        try {
            CompositePipelineSettings settings = (CompositePipelineSettings) settings();
            readFilters();
            for (String key : settings.getPipelineContext().keySet()) {
                PipelineInfo pi = settings.getPipelineContext().get(key);
                if (!filters.containsKey(key)) {
                    throw new Exception(String.format("Missing pipeline expression. [name=%s]", key));
                }
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    private void readFilters() throws Exception {
        HierarchicalConfiguration<ImmutableNode> config = config().configurationAt(__CONFIG_PATH_FILTERS);
        List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(PathFilter.__CONFIG_PATH);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            PathFilter filter = ConfigReader.read(node, PathFilter.class);
            filters.put(filter.getName(), filter);
        }
    }

    @Override
    protected Object evaluate(SourceMap data, String filter) throws Exception {
        PathFilter f = filters.get(filter);
        if (f == null) {
            throw new Exception(String.format("Specified filter not found. [name=%s]", filter));
        }
        Object ret = null;
        if (Strings.isNullOrEmpty(f.getFilter())) {
            ret = JsonPath.read(data, f.getPath());
        } else {
            Filter ff = Filter.parse(f.getFilter());
            ret = JsonPath.read(data, f.getPath(), ff);
        }
        if (ret == null) {
            DefaultLogger.debug(String.format("Filter returned null: [path=%s][filter=%s]",
                    f.getPath(), f.getFilter()));
        } else if (DefaultLogger.isTraceEnabled()) {
            DefaultLogger.trace(String.format("[PATH=%s]", f.getPath()), ret);
        }
        return ret;
    }
}
