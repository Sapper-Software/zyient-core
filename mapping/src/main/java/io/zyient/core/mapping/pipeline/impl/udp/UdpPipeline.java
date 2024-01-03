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

package io.zyient.core.mapping.pipeline.impl.udp;

import com.google.common.base.Strings;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.pipeline.PathFilter;
import io.zyient.core.mapping.pipeline.Pipeline;
import io.zyient.core.mapping.pipeline.PipelineInfo;
import io.zyient.core.mapping.pipeline.settings.CompositePipelineSettings;
import io.zyient.core.mapping.pipeline.source.SourceCompositePipeline;
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
public class UdpPipeline extends SourceCompositePipeline {

}
