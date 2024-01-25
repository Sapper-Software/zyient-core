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

package io.zyient.core.mapping.pipeline.impl.udp;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.model.ContentInfo;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.OutputContentInfo;
import io.zyient.core.mapping.readers.MappingContextProvider;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class UdpMappingContextProvider implements MappingContextProvider {
    private static final String CONFIG_PIPELINE = "pipeline";

    private String pipeline;

    @Override
    public MappingContextProvider configure(HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        pipeline = config.getString(CONFIG_PIPELINE);
        ConfigReader.checkStringValue(pipeline, getClass(), CONFIG_PIPELINE);
        return this;
    }

    @Override
    public InputContentInfo inputContext(@NonNull ContentInfo contentInfo) throws Exception {
        InputContentInfo ci = new InputContentInfo(contentInfo);
        ci.contentType(SourceTypes.JSON);
        ci.mapping(pipeline);
        return ci;
    }

    @Override
    public OutputContentInfo outputContext(@NonNull ContentInfo contentInfo) throws Exception {
        return null;
    }
}
