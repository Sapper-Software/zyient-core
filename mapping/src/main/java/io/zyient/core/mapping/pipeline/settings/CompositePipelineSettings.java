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

package io.zyient.core.mapping.pipeline.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.core.mapping.pipeline.PipelineInfo;
import io.zyient.core.mapping.pipeline.PipelineSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CompositePipelineSettings extends PipelineSettings {
    public static final String __CONFIG_PATH = "mappings";
    public static final String __CONFIG_PATH_MAPPING = "mapping";

    private Map<String, PipelineInfo> pipelineContext;

    public void read(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        HierarchicalConfiguration<ImmutableNode> cfg = config.configurationAt(__CONFIG_PATH);
        List<HierarchicalConfiguration<ImmutableNode>> nodes = cfg.configurationsAt(__CONFIG_PATH_MAPPING);
        try {
            pipelineContext = new HashMap<>();
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                PipelineInfo pi = ConfigReader.read(node, PipelineInfo.class);
                pipelineContext.put(pi.getExpression(), pi);
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
