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
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.MappingContextProvider;
import io.zyient.core.mapping.readers.ReadResponse;
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

    public abstract Pipeline configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                       @NonNull MapperFactory mapperFactory,
                                       @NonNull DataStoreManager dataStoreManager) throws ConfigurationException;

    public abstract ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception;

    public abstract void process(@NonNull SourceMap data, Context context) throws Exception;

}
