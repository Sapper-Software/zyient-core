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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ConfigValueParser;
import io.zyient.base.common.utils.ReflectionUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.lang.reflect.Field;

@Getter
@Setter
@ConfigPath(path = "mapping")
public class MappedElement {
    @Config(name = "source")
    private String sourcePath;
    @Config(name = "target")
    private String targetPath;
    @Config(name = "required", required = false, type = Boolean.class)
    private boolean mandatory = false;
    private Class<?> type;
    @Config(name = "regex")
    private String regex;
    @Config(name = "type", required = false, type = MappingType.class)
    private MappingType mappingType = MappingType.Field;

    public static MappedElement read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull Class<? extends MappedElement> type) throws Exception {
        return ConfigReader.read(xmlConfig, type);
    }
}
