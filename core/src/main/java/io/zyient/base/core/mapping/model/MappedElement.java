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

package io.zyient.base.core.mapping.model;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@ConfigPath(path = "transformation")
public class MappedElement {
    @Config(name = "source")
    private String sourcePath;
    @Config(name = "target")
    private String targetPath;
    @Config(name = "nullable", required = false, type = Boolean.class)
    private boolean nullable = true;
    private Class<?> type;
    @Config(name = "regex")
    private String regex;
    @Config(name = "type", required = false, type = MappingType.class)
    private MappingType mappingType = MappingType.Field;

    public MappedElement() {

    }

    public MappedElement(@NonNull String sourcePath,
                         @NonNull String targetPath,
                         boolean nullable,
                         @NonNull Class<?> type,
                         String regex,
                         MappingType mappingType) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.nullable = nullable;
        this.type = type;
        this.regex = regex;
        if (mappingType != null)
            this.mappingType = mappingType;
    }

    public static MappedElement read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull Class<? extends MappedElement> type) throws Exception {
        return ConfigReader.read(xmlConfig, type);
    }
}
